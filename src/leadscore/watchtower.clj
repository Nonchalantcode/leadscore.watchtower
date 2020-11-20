(ns leadscore.watchtower
  (:import [java.net URL]
           [java.io File
            InputStreamReader
            BufferedReader]
           java.util.function.Consumer)
  (:require [leadscore.spy-fu :as spy-fu]
            [leadscore.config :refer (config)]
            [leadscore.netcore :as netcore]
            [leadscore.storage :as storage
             :refer (leads-buffer
                     crawl-buffer
                     api-key
                     export-buffer
                     load-crawl-buffer
                     dump-crawl-buffer!)
             :rename {export-buffer export-leads-buffer}]
            [leadscore.functions :as functions :refer (inspect-buffer load-veto-lists)]
            [leadscore.db :refer (is-db-online)]
            [cheshire.core :as JSON]
            (ring.adapter [jetty :refer :all])
            (ring.middleware [resource :refer :all]
                             [params :refer (params-request)])
            (ring.util [response :as response])
            (ring.mock [request :as r :refer :all])
            (clojure [string :refer (join)])
            (compojure [core :refer :all]))
  (:gen-class))

(def ^:private separator (:separator config))
(def ^:private user-dir (:user-dir config))
(def ^:private resources-dir (:resources-dir config))

(set! *warn-on-reflection* true)

(defn get-active-categories [] (keys leads-buffer))

(defn reload-veto-list []
  (load-veto-lists (str resources-dir separator "vetolist")))

(defn with-content-type [response mime-type]
  (response/header response "Content-Type" mime-type))

(defn test-initial-conf
  []
   (let [spyfu-api-key (-> config :spy-fu :api-key)
         db-spec (-> config :db-spec)]
     {:validApiKey false
      :validDBCredentials (is-db-online db-spec)}))

(defn count-leads-for [category-name]
  (let [states (keys (get leads-buffer category-name))]
    (reduce (fn [total current-state]
              (let [cities-map (get-in leads-buffer [category-name current-state "cities"])
                    cities (keys cities-map)]
                (+ total
                   (apply + (map #(count (get-in leads-buffer [category-name current-state "cities" %]))
                                 cities)))))
                
            (identity 0)
            (identity states))))

(defn get-total-leads-count []
  (apply + (map count-leads-for (get-active-categories))))

(defn as-json [v]
  (-> v 
      (JSON/generate-string) 
      (response/response)
      (response/header "Content-Type" "application/json")))

(defn cors [request]
  (-> request 
   (response/header "Access-Control-Allow-Origin" "*")
   (response/header "Access-Control-Allow-Headers" "Origin, X-Requested-With, Content-Type, Accept")))

(defn- list-leads []
  (sort-by :timestamp
           >
           (map (fn [filename]
                  (let [[timestamp timezone & category-name] (reverse (.split ^String filename "_"))]
                    {:timestamp (Long/parseLong timestamp)
                     :timezone timezone
                     :filename (join " " (reverse category-name))}))
                (.list (File. ^String (:buffers-dir config))
                         (reify java.io.FilenameFilter
                           (accept [this file filename]
                             (not (.startsWith filename "out"))))))))

(defroutes routes-table
  (GET "/" req
       (resource-request (assoc req :uri "/index.html") "/public"))
  (GET "/index.html" req
       (resource-request req "/public"))
  (GET "/api/status" []
       (as-json (assoc (test-initial-conf) :leadcount (get-total-leads-count))))
  (GET "/leads/all" []
       (as-json (list-leads)))
  (GET "/leads/count" []
       (as-json {:leadcount (get-total-leads-count)}))
  (GET "/leads/categories" []
       (as-json {:categories (get-active-categories)}))
  (POST "/leads/save" request
        (do
          (try 
            (export-leads-buffer (first (get-active-categories)))
            (as-json {:message "Saved", :error false})
            (catch Exception err
              (as-json {:message (.getMessage err), :error true})))))
  (POST "/leads/discard" request
        (do
          (try (.clear leads-buffer)
               (as-json {:message "Leads were discarded", :error false})
               (catch Exception err
                 (as-json {:message (.getMessage err), :error true})))))
  (POST "/leads/upload" {:keys [params body]}
        (try
          (let [body (JSON/parse-stream (java.io.InputStreamReader. body))]
            (do (storage/save-to-buffer params body)
                (as-json {:message "stored" :leadcount (get-total-leads-count) :error false})))
          (catch Exception err
            (as-json {:message (.getMessage err), :error true})))))
          

(def app (fn [request-map]
           (-> request-map
               params-request
               routes-table
               cors)))
               

(def server (run-jetty #'app {:join? false, :port (:port config)}))
