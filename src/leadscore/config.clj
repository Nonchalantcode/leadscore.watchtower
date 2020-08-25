(ns leadscore.config
  (:require [leadscore.functions :refer (load-config)])
  (:import (java.io File FileWriter)))
  

(def ^:private separator (. File separator))
(def ^:private user-dir (. System getProperty "user.dir"))
(def ^:private user-home (. System getProperty "user.home"))
(def ^:private user-downloads-dir (str user-home separator "Downloads"))
(def ^:private reports-dir (str user-dir separator "reports"))
(def ^:private resources-dir (str user-dir separator "resources"))
(def ^:private buffers-dir (str resources-dir separator "buffers"))
(def ^:private phone-matcher #"\(?\d{3}\)?[\s\.-]\d{3}[\s\.-]\d{4}")
(def ^:private asummed-user-agent "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:73.0) Gecko/20100101 Firefox/73.0")
(def ^:private port 8000)
(def ^:private default-user-conf
  {:db-spec {:dbtype "", :dbname "", :user "", :password ""}
   :spy-fu {:api-key ""}})

(def ^:private user-conf (let [ENV (File. (str resources-dir separator ".ENV"))]
                          (if (.exists ENV)
                            (load-config ENV)
                            (do (.createNewFile ENV)
                                (with-open [ENV (FileWriter. ENV)]
                                  (.write ENV (pr-str default-user-conf)))
                                (load-config ENV)))))
  
(def config {:separator separator
             :user-dir user-dir
             :user-home user-home
             :user-downloads-dir user-downloads-dir
             :reports-dir reports-dir
             :resources-dir resources-dir
             :buffers-dir buffers-dir
             :phone-matcher phone-matcher
             :asummed-user-agent asummed-user-agent
             :db-spec (:db-spec user-conf)
             :spy-fu (:spy-fu user-conf)
             :port port})