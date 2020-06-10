(ns leadscore.storage
  (:import (java.io File
                    FileReader
                    FileWriter
                    BufferedWriter
                    BufferedReader)
           (java.util HashSet
                      LinkedList
                      HashMap)
           java.util.function.Consumer)
  (:require [clojure.inspector :refer (inspect-tree)]
            [leadscore.constants :refer (resources-dir buffers-dir separator)]
            [leadscore.functions :refer (iterate! for-each get-hostname load-config load-veto-lists noop async-call)]
            [leadscore.spy-fu :refer (valid-apiKey? await-batch) :rename {await-batch crawl-batch}]
            [leadscore.netcore :refer (get-numbers get-email-addr)]
            [clojure.string :refer (join split)]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :refer :all]))

(def ^:private out-dir (str buffers-dir separator "out"))
(def ^:private ^:const conf (load-config (str resources-dir separator ".ENV")))
;; Defines the leads-buffer which is just a HashMap containing the list of urls that have been crawled.
;; The leads-buffer organizes leads by category, then by state (first, state-wide leads), and then by city
;; The leads-buffer isn't 'curated'. It doesn't guarantee that all urls are unique; that guarantee must be provided by the 
;; client-side crawler program. The leads-buffer needs to be filtered against leads that already exist in the database
;; for each particular State, and then for each particular city to ensure non-duplicated leads are being crawled.

(def ^:private leads-buffer (HashMap.))
(def ^:private db-leads-buffer (HashMap.))
(def ^:private veto-list ^HashSet (load-veto-lists (str resources-dir separator "vetolist")))
(def ^:private db-spec (:db-spec conf))
(def ^:private crawl-buffer (HashMap.))

(defn- inspect-buffer [buffer] (inspect-tree buffer))
(defn- view-keys [] (.keySet leads-buffer))

(defn- get-in! [^java.util.HashMap m & ks]
  (reduce (fn [acc curr] (.get acc curr)) m ks))

(defn- in-vetolist? [^HashSet veto-list ^String url]
  (.contains veto-list (get-hostname url)))

(defn- get-lead-id [db-spec lead-url category-id state-id]
  (-> (jdbc/query db-spec ["SELECT lead_id FROM leads INNER JOIN 
                        category USING (category_id) INNER JOIN
                        state USING (state_id) WHERE lead_url = ? AND
                        category_id = ? AND
                        state_id = ?" lead-url category-id state-id])
      (first)
      (:lead_id)))

(defn- get-state-id [db-spec state-name]
  (-> (jdbc/query db-spec ["SELECT state_id FROM state WHERE state_name = ?" state-name])
      (first)
      (:state_id)))

(defn- get-category-id [db-spec category]
  (-> (jdbc/query db-spec ["SELECT category_id FROM category WHERE category_name = ?" category])
      (first)
      (:category_id)))

(defn- insert-category [db-spec category-name]
  (-> (jdbc/insert! db-spec :category {:category_name category-name})
      (first)
      (:generated_key)))

(defn- get-city-id [db-spec city]
  (-> (jdbc/query db-spec ["SELECT city_id FROM city WHERE city_name = ?" city])
      (first)
      (:city_id)))

(defn- insert-city [db-spec state-name city-name]
  (let [state-id (get-state-id db-spec state-name)]
    (if (nil? state-id)
      (throw (IllegalArgumentException. (str "No state in database with name <" state-name ">")))
      (-> (jdbc/insert! db-spec :city {:state_id state-id :city_name city-name})
          (first)
          (:generated_key)))))

(defn- query-leads
  "Queries the 'leads' table for leads associated to a particular category from the 'category' table and also
   for leads associated to a particular state from the 'state' table. The city parameter is optional, but highly-recommended to use for more location-specific leads and faster querying. 
   Returns a java.util.HashSet"
  ([category state] (query-leads category state nil))
  ([category state city]
   (let [param-query1 ["SELECT lead_url FROM leads INNER JOIN category USING (category_id) 
                        INNER JOIN state USING (state_id) 
                        WHERE category_name = ?
                        AND state_name = ?"]
         param-query2 ["SELECT lead_url FROM leads INNER JOIN category USING (category_id)
                        INNER JOIN state USING (state_id)
                        INNER JOIN city USING (state_id)
                        WHERE category_name = ?
                        AND state_name = ?
                        AND city_name = ?"]
         reducer-fn (fn [^java.util.HashSet acc {:keys [lead_url]}]
                      (doto acc (.add lead_url)))]
     (if (nil? city)
       (reduce reducer-fn
               (java.util.HashSet.)
               (jdbc/reducible-query db-spec (conj param-query1 category state) {:raw? true}))
       (reduce reducer-fn
               (java.util.HashSet. 1000)
               (jdbc/reducible-query db-spec (conj param-query2 category state city) {:raw? true}))))))

(defn save-to-buffer
  "Saves the contents from the incoming InputStream into the leads-buffer var which is a hashmap."
  [{:strs [category state city]} ^java.io.InputStream in]
  (cond
    ;; If category doesn't exists in db-leads-buffer, register it together with the State.
    (nil? (get-in! db-leads-buffer category))
    (do (.put db-leads-buffer category (HashMap.))
        (.put (get-in! db-leads-buffer category) state (HashMap.))
        ;; Get the state-wide leads in put them in a "leads" slot
        (.put (get-in! db-leads-buffer category state) "leads" (query-leads category state))
        (if (seq city)
          (.put (get-in! db-leads-buffer category state) city (query-leads category state city))
          (noop)))
    ;; If category exists in db-leads-buffer, but the State doesnt, register it)
    (nil? (get-in! db-leads-buffer category state))
    (do (.put (get-in! db-leads-buffer category) state (HashMap.))
        (.put (get-in! db-leads-buffer category state) "leads" (query-leads category state))
        (if (seq city)
          (.put (get-in! db-leads-buffer category state) city (query-leads category state city))))
    ;; If category and state exist in db-leads-buffer but not the city; register it.
    (not (nil? (get-in! db-leads-buffer category state)))
    (if (seq city)
      (.put (get-in! db-leads-buffer category state) city (query-leads category state city))))

  (let [leads (HashSet. 200)
        db-state-wide-results ^HashSet (get-in! db-leads-buffer category state "leads")
        city-state-results ^HashSet (or (get-in! db-leads-buffer category state city)
                                        (HashSet.))]

    (for-each in (fn [lead-url]
                   (if (or (in-vetolist? veto-list lead-url)
                           (.contains db-state-wide-results lead-url)
                           (.contains city-state-results lead-url))
                     (println "Omitting" lead-url)
                     (.add leads lead-url))))

      ;; If the current category being processed is not in #'leads-buffer, add it.
    (if (nil? (get-in! leads-buffer category))
      (do (.put leads-buffer category (HashMap.))
          (doto (get-in! leads-buffer category)
            (.put state (HashMap.)))
          (doto (get-in! leads-buffer category state)
            (.put "leads" (HashSet.))
            (.put "cities" (HashMap.))))
        ;; If the category exists. Test if the current State is part of that category
      (if (nil? (get-in! leads-buffer category state))
        (do (doto (get-in! leads-buffer category)
              (.put state (HashMap.)))
            (doto (get-in! leads-buffer category state)
              (.put "leads" (HashSet.))
              (.put "cities" (HashMap.))))))

      ;; If the city binding is not an empty string
    (if (not (empty? city))
      (cond
          ;; If the current city isn't already in #'leads-buffer
        (nil? (get-in! leads-buffer category state "cities" city))
        (.put (get-in! leads-buffer category state "cities") city leads)

          ;; Both the state and city are in #'leads-buffer. Merge values.
        (not (nil? (get-in! leads-buffer category state "cities" city)))
        (.addAll (get-in! leads-buffer category state "cities" city) leads))

          ;; If the city binding is an empty string
      (.addAll (get-in! leads-buffer category state "leads") leads))
    :done!))

(defn- write-table-head [^java.io.Writer csv-file & column-names]
  (.write csv-file (join "," column-names))
  (.newLine csv-file))

(defn export-buffer
  "Writes to the /resources/buffers directory a .CSV file with the contents of the #'leads-buffer var"
  [category & {:keys [tzone] :or {tzone "nospec"}}]
  (let [mappings ^java.util.HashMap (.remove leads-buffer category)
        all-states (map #(identity [% (.get mappings %)]) (.keySet mappings))
        results-folder (doto (java.io.File. (str buffers-dir
                                                 separator
                                                 category
                                                 "-"
                                                 tzone
                                                 "-"
                                                 (System/currentTimeMillis)))
                         (.mkdirs))
        all-leads (doto (File. results-folder "unified.csv")
                    (.createNewFile))]

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

;; Reads a .csv into the crawl-buffer HashMap with the following shape: 
;; {"category": category,
;;  "url": {
;;    city: city,
;;    state: state,
;;    seo: seo,
;;    ppc: ppc,
;;    number: number }
;;   "urls": []}
;; Gather all the urls from the HashMap above for which number is nil. 
;; Crawl them using the phone-number crawling function
;; After the crawl is finished, insert the values into the map above. 

(defmulti load-crawl-buffer
  "Loads leads from a .CSV file and writes its contents to the #'crawl-buffer var for further processing,
   that is, to gather phone numbers or spy fu information."
  (fn [source category] (type source)))

(defmethod load-crawl-buffer java.lang.String
  [source category] (load-crawl-buffer (File. source) category))

(defmethod load-crawl-buffer java.io.File
  [source category]
  (with-open [handle (-> source (FileReader.) (BufferedReader.))]
    (.readLine handle) ;; The column names for the table. We discard this value.
    (.put crawl-buffer "category" category)
    (.put crawl-buffer "urls" (HashSet.))
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

(defn populate-crawl-buffer!
  "Fills the #'crawl-buffer var with either phone-number information
   for the urls provided, or seo and ppc data from spy fu, or both"
  [opts]
  (let [{:keys [both? spy-fu? phone? api-key]} opts
        filtered-crawl-list (if (.isEmpty crawl-buffer)
                              (identity nil)
                              (reduce (fn [acc curr]
                                        (if (nil? (get-in! crawl-buffer curr "phone"))
                                          (conj acc curr)
                                          (noop)))
                                      (vector)
                                      (.get crawl-buffer "urls")))
        search-numbers (fn [urls-coll]
                         (doall (map (fn [[url {phone-number :number}]]
                                       (.put (.get crawl-buffer url) "phone" (apply str phone-number)))
                                     (get-numbers urls-coll))))
        search-spyfu (fn []
                       (let [all-urls (.get crawl-buffer "urls")
                             spy-fu-results (reduce (fn [acc curr] (merge acc (crawl-batch api-key curr)))
                                                    (hash-map)
                                                    (partition 10 10 nil all-urls))]
                         (doseq [curr all-urls]
                           (let [spy-fu-metrics (spy-fu-results curr)]
                             (.put (.get crawl-buffer curr) "ppc" (and
                                                                   spy-fu-metrics
                                                                   (-> (.get spy-fu-metrics "ppc_budget")
                                                                       (or 0)
                                                                       (.longValue))))
                             (.put (.get crawl-buffer curr) "seo" (and
                                                                   spy-fu-metrics
                                                                   (-> (.get spy-fu-metrics "seo_value")
                                                                       (or 0)
                                                                       (.longValue))))))))]
    (cond
      (.isEmpty crawl-buffer) (noop)

      (and (identity both?) (nil? api-key))
      (throw (IllegalArgumentException. "No API-KEY was provided."))

      (and (identity both?) (not (valid-apiKey? api-key)))
      (throw (IllegalArgumentException. "Provided API-KEY is not valid."))

      (identity both?) (do (search-numbers filtered-crawl-list)
                           (search-spyfu))

      (identity phone?) (search-numbers filtered-crawl-list)

      (and (identity spy-fu?) (nil? api-key))
      (throw (IllegalArgumentException. "No API-KEY was provided."))

      (and (identity spy-fu?) (not (valid-apiKey? api-key)))
      (throw (IllegalArgumentException. "Provided API-KEY is not valid."))

      (identity spy-fu?) (search-spyfu))
    :done!))

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
  (with-open [out-all (get-crawl-buffer-writer (.get crawl-buffer "category") "all")
              out-success (get-crawl-buffer-writer (.get crawl-buffer "category") "success")]
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

(defmulti dump-on-db
  "Dumps the contents of the provided source onto the database specified by db-spec.
   If source is a HashMap, it must have a shape that conforms to #'crawl-buffer. All other sources
   must be able to be transformed to adapt to the shape of #'crawl-buffer."
  (fn [source db-spec] (type source)))

(defmethod dump-on-db java.util.HashMap
  [source db-spec]
  (let [entries (.get source "urls")
        category (.get source "category")
        category-id (or (get-category-id db-spec category) (insert-category db-spec category))
        lead-data (LinkedList.)]
    ;; inserts url, phone number, and state and city data.
    (jdbc/insert-multi! db-spec :leads [:category_id :state_id :lead_url :lead_phone]
                        (persistent! (reduce (fn [results current-lead]
                                               (let [lead-map (.get source current-lead)
                                                     state (.get lead-map "state")
                                                     state-id (get-state-id db-spec state)
                                                     city (.get lead-map "city")
                                                     city-id (or (get-city-id db-spec city) (insert-city db-spec state city))]
                                                 (.add lead-data [current-lead category-id state-id])
                                                 (conj! results [category-id state-id current-lead (.get lead-map "phone")])))
                                             (transient [])
                                             entries)))
    ;; inserts spy-fu data. 
    (jdbc/insert-multi! db-spec :spyfu_data [:lead_id :data_seo_value :data_ppc_value]
                        (persistent! (reduce (fn [results [url category-id state-id]]
                                               (let [seo (get-in! source url "seo")
                                                     ppc (get-in! source url "ppc")]
                                                 (conj! results [(get-lead-id db-spec url category-id state-id)
                                                                 (if (empty? seo) 0 (Integer/valueOf seo))
                                                                 (if (empty? ppc) 0 (Integer/valueOf ppc))])))
                                             (transient [])
                                             lead-data)))
    :done!))
