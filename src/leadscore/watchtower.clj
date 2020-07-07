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
            [leadscore.functions :as functions]
            [cheshire.core :as JSON]
            (ring.adapter [jetty :refer :all])
            (ring.middleware [resource :refer :all]
                             [params :refer (params-request)])
            (ring.util [response :as response])
            (ring.mock [request :as r :refer :all])
            (compojure [core :refer :all]))
  (:gen-class))

(set! *warn-on-reflection* true)

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