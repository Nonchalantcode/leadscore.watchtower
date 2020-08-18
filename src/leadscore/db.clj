(ns leadscore.db
  (:require [leadscore.functions :refer (if-bound)]
            [clojure.java.jdbc :as jdbc]))

#_(defn- query-leads
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

(defn get-lead-id [db-spec lead-url category-id state-id]
  (-> (jdbc/query db-spec ["SELECT lead_id FROM leads INNER JOIN 
                        category USING (category_id) INNER JOIN
                        state USING (state_id) WHERE lead_url = ? AND
                        category_id = ? AND
                        state_id = ?" lead-url category-id state-id])
      (first)
      (:lead_id)))

(defn get-state-id [db-spec state-name]
  (-> (jdbc/query db-spec ["SELECT state_id FROM state WHERE state_name = ?" state-name])
      (first)
      (:state_id)))

(defn get-category-id [db-spec category]
  (-> (jdbc/query db-spec ["SELECT category_id FROM category WHERE category_name = ?" category])
      (first)
      (:category_id)))

(defn insert-category [db-spec category-name]
  (-> (jdbc/insert! db-spec :category {:category_name category-name})
      (first)
      (:generated_key)))

(defn get-city-id [db-spec city]
  (-> (jdbc/query db-spec ["SELECT city_id FROM city WHERE city_name = ?" city])
      (first)
      (:city_id)))

(defn get-timezone-id [db-spec timezone-name]
  (-> (jdbc/query db-spec ["SELECT timezone_id FROM timezone WHERE timezone_name = ?" timezone-name])
      (first)
      (:timezone_id)))

(defn insert-city [db-spec state-name city-name]
  (let [state-id (get-state-id db-spec state-name)]
    (if (nil? state-id)
      (throw (IllegalArgumentException. (str "No state in database with name <" state-name ">")))
      (-> (jdbc/insert! db-spec :city {:state_id state-id :city_name city-name})
          (first)
          (:generated_key)))))

(def faster-get-state-id (memoize get-state-id))

(def faster-get-city-id (memoize get-city-id))

(defmulti dump-on-db
  "Dumps the contents of the provided source onto the database specified by db-spec.
   If source is a HashMap, it must have a shape that conforms to #'crawl-buffer. All other sources
   must be able to be transformed to adapt to the shape of #'crawl-buffer."
  (fn [source db-spec] (type source)))

(defmethod dump-on-db java.util.HashMap
  [source db-spec]
  (let [entries (.get source "urls")
        category (.get source "category")
        category-id (or (get-category-id db-spec category)
                        (insert-category db-spec category))
        timezone-id (get-timezone-id db-spec (.get source "timezone"))]
    ;; inserts url, phone number, and state and city data.
    (jdbc/insert-multi! db-spec :leads [:category_id :timezone_id :state_id :city_id :lead_url :lead_phone :seo_value :ppc_value]
                        (persistent! (reduce (fn [results lead-url]
                                               (let [lead-map (.get source lead-url)
                                                     state-id (faster-get-state-id db-spec (.get lead-map "state"))
                                                     city-id (faster-get-city-id db-spec (.get lead-map "city"))
                                                     seo (if-bound seo (and (not= nil :binding)
                                                                            (not (empty? :binding)))
                                                                   {:binding (get lead-map "seo") :else-binding 0}
                                                                   seo)
                                                     ppc (if-bound ppc (and (not= nil :binding)
                                                                            (not (empty? :binding)))
                                                                   {:binding (get lead-map "ppc") :else-binding 0}
                                                                   ppc)
                                                     phone (if-bound phone (and (not= nil :binding)
                                                                                (not (empty? :binding)))
                                                                     {:binding (get lead-map "phone") :else-binding "n/a"}
                                                                     phone)]
                                                 (conj! results [category-id timezone-id state-id city-id lead-url phone seo ppc])))
                                             (transient [])
                                             entries)))))
