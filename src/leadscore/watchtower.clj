(ns leadscore.watchtower
  (:import [java.net URL]
           [java.io File
            InputStreamReader
            BufferedReader]
           java.util.function.Consumer)
  (:require [leadscore.spy-fu :as spy-fu]
            [leadscore.constants :refer (separator resources-dir)]
            [leadscore.netcore :as netcore]
            [leadscore.report :as report]
            [leadscore.storage :as storage]
            [cheshire.core :as JSON]
            (ring.adapter [jetty :refer :all])
            (ring.middleware [resource :refer :all]
                             [params :refer (params-request)])
            (ring.util [response :as response])
            (ring.mock [request :as r :refer :all])
            (compojure [core :refer :all]))
  (:gen-class))

(set! *warn-on-reflection* true)

(defn- auth-user [^java.io.ByteArrayInputStream body]
  (let [arr (byte-array 20)]
    (.read body arr)
    (if (spy-fu/valid-apiKey? (-> arr String. .trim))
      (do
        (println "Validation passed.")
        (def query-api (partial spy-fu/query-api (-> arr String. .trim)))
        (response/response "Authentication succeeded."))
      (-> (response/response "Authentication failed.")
          (response/status 401)))))

(defn- get-spyfu-metrics
  [^String host]
  (let [p (promise)]
    (future (let [query-results (JSON/decode (query-api host))
                  query-info (and query-results
                                  {:ppc_budget (query-results "ppc_budget")
                                   :num_seo_kw (query-results "num_seo_kw")
                                   :num_ppc_kw (query-results "num_ppc_kw")
                                   :seo_value (query-results "seo_value")
                                   :seo_clicks (query-results "seo_clicks")
                                   :ppc_clicks (query-results "ppc_links")})]
              (deliver p (assoc {} (keyword host) query-info))))
    p))


(defn- get-numbers [^java.io.InputStream listings]
  (with-open [listings (-> listings InputStreamReader. BufferedReader.)]
    (let [curated-list (loop [current (.trim (.readLine listings)) final []]
                         (if (or (nil? current))
                           final
                           (if (zero? (.length current))
                             (recur (.readLine listings) final)
                             (recur (.readLine listings) (conj final current)))))]
      (response/response (JSON/encode (netcore/get-numbers curated-list))))))


(defn proc-upload [^java.io.InputStream f]
  (with-open [contents (-> (InputStreamReader. f) (BufferedReader.) (.lines))]
    (let [results (atom {})]
      (.forEach contents (reify Consumer
                           (accept [this val]
                             (if (netcore/isCanonicalUrl val)
                               (swap! results
                                      merge
                                      (deref (get-spyfu-metrics (.getHost (URL. val)))))
                               (let [host (.getHost (-> val
                                                        (netcore/toCanonicalUrl)
                                                        (URL.)))]
                                 (swap! results
                                        merge
                                        (deref (get-spyfu-metrics host))))))))
      (response/response (JSON/encode @results)))))

(defn export-all [information]
  (let [filename (str (gensym "all-results"))
        extension ".csv"
        outdir (File. (str resources-dir separator "reports"))
        temp (File/createTempFile filename extension outdir)]

    (try
      (with-open [info-reader ^BufferedReader (-> information InputStreamReader. BufferedReader.)
                  fwriter (java.io.FileWriter. temp true)]
        (loop [info ^String (.readLine info-reader)]
          (if (nil? info)
            temp
            (do (.write fwriter info) (recur ^String (.readLine info-reader))))))
      (catch Exception ex
        (println (.getMessage ex))
        (println "An Exception occurred")))))

(defn with-params? [m & params]
  (= (keys m) params))

(defroutes app
  (GET "/" []
    (File. (str resources-dir separator "index.html")))
  (GET ["/:asset" :asset #"[^\/\\]+\.[\w\d]+"] [asset]
    (File. (str resources-dir separator asset)))
  (GET ["/media/:asset" :asset #".+\.[\w\d]+"] [asset]
    (File. (str resources-dir separator "media" separator asset)))
  (GET ["/css/:sheet" :sheet #".+\.css"] [sheet]
    (File. (str resources-dir separator "css" separator sheet)))
  (GET ["/js/:script" :script #".+\.js"] [script]
    (File. (str resources-dir separator "js" separator script)))
  (POST "/" {:keys [body query-string] :or {body "000000"} :as req-map}
    (let [parsed-params (-> req-map params-request :query-params)
          with? (partial with-params? parsed-params)]
      (cond
        (= parsed-params {"auth" "true"}) (auth-user body)
        (= parsed-params {"upload" "true"}) (proc-upload body)
        (= parsed-params {"get_numbers" "true"}) (get-numbers body)
        (= parsed-params {"export_results" "true" "all" "true"}) (response/response (export-all body))

        (every? (set (keys parsed-params)) ["buffer" "state" "category"])
        (do (storage/save-to-buffer parsed-params body)
            (response/response "ok"))

        (with? "generate_report") (-> (response/response (report/generate-report body :type (parsed-params "generate_report"))))
        (with? "generate_report" "filename") (-> (response/response (report/generate-report body :type (parsed-params "filename"))))))))


(def server (run-jetty #'app {:join? false, :port 3000}))