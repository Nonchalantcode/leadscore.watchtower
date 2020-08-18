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
            [cheshire.core :as JSON]
            (ring.adapter [jetty :refer :all])
            (ring.middleware [resource :refer :all]
                             [params :refer (params-request)])
            (ring.util [response :as response])
            (ring.mock [request :as r :refer :all])
            (compojure [core :refer :all]))
  (:gen-class))

(def ^:private separator (:separator config))
(def ^:private user-dir (:user-dir config))
(def ^:private resources-dir (:resources-dir config))

(set! *warn-on-reflection* true)

(defn view-active-categories [] (keys leads-buffer))

(defn crawl-phone-info [] (storage/populate-crawl-buffer! {:phone? true}))

(defn crawl-spyfu-info [api-key] (storage/populate-crawl-buffer! {:spy-fu? true :api-key api-key}))

(defn reload-veto-list []
  (load-veto-lists (str resources-dir separator "vetolist")))

(defroutes routes-table
  (GET "/" []
    (response/response "Hello, world!"))
  (POST "/buffer" {:keys [params body]}
    (do (storage/save-to-buffer params body)
        (response/response "Stored"))))

(def app (fn [request-map]
           (-> request-map
               params-request
               routes-table)))

(def server (run-jetty #'app {:join? false, :port 3000}))
