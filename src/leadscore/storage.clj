(ns leadscore.storage
  (:import (java.io File
                    FileReader
                    FileWriter
                    BufferedWriter
                    BufferedReader)
           (java.util HashSet
                      HashMap)
           java.util.function.Consumer)
  (:require [leadscore.config :refer (config)]
            [leadscore.functions :refer (iterate!
                                         read-lines
                                         get-hostname
                                         load-veto-lists
                                         noop
                                         inspect-buffer
                                         get-in!)]
            [leadscore.db :refer :all]
            [leadscore.netcore :refer (crawl-urls get-spyfu-info)]
            [clojure.string :refer (join split)]
            [cheshire.core :refer :all]))

(def ^:private resources-dir (:resources-dir config))
(def ^:private buffers-dir (:buffers-dir config))
(def ^:private separator (:separator config))
(def ^:private out-dir (str buffers-dir separator "out"))
(def db-spec (:db-spec config))
(def api-key (-> config :spy-fu :api-key))

(def ^:private db-leads-buffer (HashMap.))
(def ^:private veto-list ^HashSet (load-veto-lists (str resources-dir separator "vetolist")))
(def leads-buffer (HashMap.))
(def crawl-buffer (HashMap.))

(defn- in-vetolist? [^HashSet veto-list ^String url]
  (.contains veto-list (get-hostname url)))

(defn- populate-db-leads-buffer [category state city]
  (cond
    (nil? (get-in! db-leads-buffer category))
    (do (.put db-leads-buffer category (HashMap.))
        (.put (get-in! db-leads-buffer category) state (HashMap.))
        (.put (get-in! db-leads-buffer category state) "leads" (query-leads db-spec category state))
        (if (seq city)
          (.put (get-in! db-leads-buffer category state) city (query-leads db-spec category state city))
          (noop)))
    (nil? (get-in! db-leads-buffer category state))
    (do (.put (get-in! db-leads-buffer category) state (HashMap.))
        (.put (get-in! db-leads-buffer category state) "leads" (query-leads db-spec category state))
        (if (seq city)
          (.put (get-in! db-leads-buffer category state) city (query-leads db-spec category state city))))
    (not (nil? (get-in! db-leads-buffer category state)))
    (if (seq city)
      (.put (get-in! db-leads-buffer category state) city (query-leads db-spec category state city)))))

(defn save-to-buffer
  "Saves the contents from the incoming Urls-Coll into the leads-buffer var which is a hashmap."
  [{:strs [category state city]} urls-coll]
  (let [leads (HashSet. 200)]
    (try
      (populate-db-leads-buffer category state city)
      (let [db-state-wide-results ^HashSet (get-in! db-leads-buffer category state "leads")
            city-state-results ^HashSet (or (get-in! db-leads-buffer category state city)
                                            (HashSet.))]
        (doseq [url urls-coll]
          (if (or (in-vetolist? veto-list url)
                  (.contains db-state-wide-results url)
                  (.contains city-state-results url))
            (println "Omitting" url)
            (.add leads url))))
        
      (catch Exception e
        (doseq [url urls-coll]
          (if (in-vetolist? veto-list url)
                                   (println "Omitting" url)
                                   (.add leads url))))
      
      (finally
        (if (nil? (get-in! leads-buffer category))
          (do (.put leads-buffer category (HashMap.))
              (doto (get-in! leads-buffer category)
                (.put state (HashMap.)))
              (doto (get-in! leads-buffer category state)
                (.put "leads" (HashSet.))
                (.put "cities" (HashMap.)))))
        (if (nil? (get-in! leads-buffer category state))
            (do (doto (get-in! leads-buffer category)
                  (.put state (HashMap.)))
                (doto (get-in! leads-buffer category state)
                  (.put "leads" (HashSet.))
                  (.put "cities" (HashMap.)))))
        (if (not (empty? city))
          (cond
            (nil? (get-in! leads-buffer category state "cities" city))
            (.put (get-in! leads-buffer category state "cities") city leads)
            (not (nil? (get-in! leads-buffer category state "cities" city)))
            (.addAll (get-in! leads-buffer category state "cities" city) leads))
          (.addAll (get-in! leads-buffer category state "leads") leads))))))

(defn- write-table-head [^java.io.Writer csv-file & column-names]
  (.write csv-file (join "," column-names))
  (.newLine csv-file))

(defn export-buffer
  "Writes to the /resources/buffers directory a .CSV file with the contents of the #'leads-buffer var"
  [category & {:keys [timezone] :or {timezone "nospec"}}]
  (let [mappings ^java.util.HashMap (.remove leads-buffer category)
        all-states (map #(identity [% (.get mappings %)]) (.keySet mappings))
        results-folder (doto (java.io.File. (str buffers-dir
                                                 separator
                                                 (join "_" (split category #"\s"))
                                                 "_"
                                                 timezone
                                                 "_"
                                                 (System/currentTimeMillis)))
                         (.mkdirs))
        all-leads (doto (File. results-folder "unified.csv") (.createNewFile))]

    ;; Write the name of the columns in the resulting .csv file
    (with-open [all-leads-handle (-> all-leads (FileWriter. true) (BufferedWriter.))]
      (write-table-head all-leads-handle "Category" "Lead URL" "State" "City" "SEO" "PPC" "Phone #"))

    ;; process individual states.
    (doseq [[state-name state-map-info] all-states]
      ;; [Atlanta {cities: {}, leads: []}]
      (with-open [all-leads-handle (-> all-leads (FileWriter. true) (BufferedWriter.))]
        (iterate! (get-in! state-map-info "leads") lead-url
                  (.write all-leads-handle (str category "," lead-url "," state-name))
                  (.newLine all-leads-handle)))
      (doall (map (fn [city-name]
                    (with-open [curr-state-handle (-> (doto (File. results-folder
                                                                   (str state-name "-" city-name ".csv"))
                                                        (.createNewFile))
                                                      (FileWriter. true)
                                                      (BufferedWriter.))
                                all-leads-handle (-> all-leads (FileWriter. true) (BufferedWriter.))]
                      (write-table-head curr-state-handle "Category" "Lead URL" "State" "City" "SEO", "PPC" "Phone #")
                      (iterate! (get-in! state-map-info "cities" city-name) lead-url
                                (.write all-leads-handle (str category "," lead-url "," state-name "," city-name))
                                (.newLine all-leads-handle)
                                (.write curr-state-handle (str category "," lead-url "," state-name "," city-name))
                                (.newLine curr-state-handle))))
                  (.keySet (get-in! state-map-info "cities")))))))

(defmulti load-crawl-buffer
  "Loads leads from a .CSV file and writes its contents to the #'crawl-buffer var for further processing,
   that is, to gather phone numbers or spy fu information."
  (fn [source category timezone] (type source)))

(defmethod load-crawl-buffer java.lang.String
  [source category timezone] (load-crawl-buffer (File. source) category timezone))

(defmethod load-crawl-buffer java.io.File
  [source category timezone]
  (with-open [handle (-> source (FileReader.) (BufferedReader.))]
    (.readLine handle) ;; The column names for the table. We discard this value.
    (doto crawl-buffer
      (.put "category" category)
      (.put "timezone" timezone)
      (.put "urls" (HashSet.)))
    
    (.forEach (.lines handle) (reify Consumer
                                (accept [this v]
                                  (let [[_ url state city seo ppc phone] (split v #",")]
                                    (.put crawl-buffer url (HashMap.))
                                    (.add (get-in! crawl-buffer "urls") url)
                                    (doto (get-in! crawl-buffer url)
                                      (.put "state" state)
                                      (.put "city" city)
                                      (.put "seo" seo)
                                      (.put "ppc" ppc)
                                      (.put "phone" phone))))))))

(defn populate-crawl-buffer! [& {:keys [:type :api-key]}]
  (let [urls (.get crawl-buffer "urls")]
    (condp = type
      :spyfu (if (nil? api-key)
               (identity nil)
               (doseq [[url {:strs [ppc_budget seo_value]}] (get-spyfu-info api-key urls)]
                 (doto (.get crawl-buffer url)
                   (.put "ppc" (if (nil? ppc_budget) ppc_budget (long ppc_budget)))
                   (.put "seo" (if (nil? seo_value) seo_value (long seo_value))))))
      :phone (doseq [[url {phone :phone}] (crawl-urls urls :opt :phone)]
               (.put (.get crawl-buffer url) "phone" phone)))))

(defn- get-crawl-buffer-writer [category filename]
  (-> (str out-dir separator category filename (. System currentTimeMillis) (identity ".csv"))
      (File.)
      (FileWriter. true)
      (BufferedWriter.)))

(defn dump-crawl-buffer!
  "Write the contents of #'leadscore.storage/crawl-buffer to a .CSV file (creating it in
   the process) and saving it to the /out directoy inside the /buffers directory for this
   program"
  []
  (with-open [out-all (get-crawl-buffer-writer
                       (.get crawl-buffer "category")
                       (str "-all-" (.get crawl-buffer "timezone-")))
              out-success (get-crawl-buffer-writer
                           (.get crawl-buffer "category")
                           (str "-success-" (.get crawl-buffer "timezone")))]
    
    (write-table-head out-all "Category" "Lead URL" "State" "City" "SEO", "PPC" "Phone #")
    (write-table-head out-success "Category" "Lead URL" "State" "City" "SEO", "PPC" "Phone #")
    (doseq [curr-lead (.get crawl-buffer "urls")]
      (let [{:strs [ppc city phone state seo]} (.get crawl-buffer curr-lead)
            category (.get crawl-buffer "category")]
        (.write out-all (str category "," curr-lead "," state "," city "," seo "," ppc "," phone))
        (.newLine out-all)
        (if (or (empty? (get-in! crawl-buffer curr-lead "phone"))
                (= "503" (get-in! crawl-buffer curr-lead "phone")))
          (noop)
          (do (.write out-success (str category "," curr-lead "," state "," city "," seo "," ppc "," phone))
              (.newLine out-success)))))))
