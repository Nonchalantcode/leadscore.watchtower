(ns leadscore.netcore
  (:import [java.net URL HttpURLConnection MalformedURLException]
           [java.io
            InputStream
            InputStreamReader
            BufferedReader
            IOException]
           [java.util
            Date
            LinkedList])
  (:require (clojure [string :refer (replace-first starts-with?)])
            [cheshire.core :as JSON]
            (leadscore [config :refer (config)])))

(def ^:private ^:const default-phone-info {:number nil, :latency 0})
(def ^:private ^:const default-email-info {:email nil, :latency 0})
(def ^:private phone-matcher (:phone-matcher config))
(def ^:private asummed-user-agent (:asummed-user-agent config))

(set! *warn-on-reflection* true)

(defmacro try-match [str-source & regexes]
  `(or ~@(map (fn [reg] `(re-find ~reg ~str-source)) regexes)))

(defmacro ends-with [str-value & suffixes]
  `(or ~@(map (fn [v] `(. ~str-value ~'endsWith ~v)) suffixes)))

(defn is-valid-URL? [^String URL]
  (try
    (if (URL. URL) true)
    (catch MalformedURLException e false)))

(defn prepend-protocol
  [^String url]
  (cond
    (starts-with? url "http") url
    :else (str "http://" url)))

(defn- are-same-numbers [& numbers]
  (= (map #(apply str (re-seq #"\d+" %)) numbers)))

(defn is-false-number [^String number-str & patterns]
  (some #(.startsWith number-str %) patterns))

(defn- first-two-results [phone-urls-coll]
  (let [first-two (take 2 (set (filter #(not (is-false-number % "000" "(999)")) phone-urls-coll)))]
    (if (apply are-same-numbers first-two)
      (take 1 first-two)
      (identity first-two))))

(defn get-phone-number [page-html]
  (if (or (nil? page-html) (empty? page-html))
    nil
    (first-two-results (re-seq phone-matcher page-html))))

(defn get-email-address [page-html]
  (if (or (nil? page-html) (empty? page-html))
    nil
    (try-match page-html #"[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+\.\w{2,4}")))

(defmacro open-HTTP-connection [^String URL & forms]
  (let [url-instance (gensym "url-instance")
        http-connection (gensym "http-connection")]
    `(let [~url-instance (if (is-valid-URL? ~URL)
                           (URL. ~URL)
                           (URL. (prepend-protocol ~URL)))
           ~http-connection (.openConnection ^java.net.URL ~url-instance)]
       (do ~@(map (fn [[method & args]] (list* method http-connection args))
                  forms)
           (.connect ~http-connection)
           (identity ~http-connection)))))

(defn open-inputstream [^java.net.URLConnection connection]
  (let [location (.getHeaderField connection "Location")]
    (if (nil? location)
      (.getInputStream connection)
      (open-inputstream (open-HTTP-connection location
                                              (.setConnectTimeout 7000)
                                              (.setReadTimeout 5000)
                                              (.setRequestProperty
                                               "User-Agent" (:asummed-user-agent config)))))))

(defn read-page-source [^String URL]
  (try (with-open [reader (-> (open-inputstream
                               (open-HTTP-connection URL
                                                     (.setConnectTimeout 7000)
                                                     (.setReadTimeout 5000)
                                                     (.setRequestProperty
                                                      "User-Agent" (:asummed-user-agent config))))
                              (InputStreamReader.)
                              (BufferedReader.))]
         (let [lines-stream (.lines reader)
               source-html (StringBuilder. 1000)]
           (.forEach lines-stream (reify java.util.function.Consumer
                                    (accept [this line]
                                      (.append source-html ^String line))))
           (.toString source-html)))
       (catch Exception err
         (println (.getMessage err)))))

(defn- lazy-read-page-source-urls [urls-coll f]
  (if (nil? urls-coll)
    (identity nil)
    (lazy-seq
     (cons (f (first urls-coll))
           (lazy-read-page-source-urls (next urls-coll) f)))))

(defn request-webpage-source
  "Return a lazyseq of urls mapped to the html source associated to the pages' urls"
  [urls-coll & {:keys [concurrent-ops] :or {concurrent-ops 3}}]
  (let [initial-count (if (> (count urls-coll) concurrent-ops) concurrent-ops (count urls-coll))
        counter (atom 0)
        initial (lazy-read-page-source-urls urls-coll
                                            (fn [url]
                                              (future
                                                (try
                                                  (let [url-source (read-page-source url)]
                                                    (swap! counter inc)
                                                    {:info {:url url, :html url-source}})
                                                  (catch Exception err
                                                    (swap! counter inc)
                                                    {:info {:url url, :html nil}})))))
                                                

        results (promise)]
    (doall (take initial-count initial))
    (add-watch counter :counter (fn [_k _r oldcount newcount]
                                  (println "Crawled so far: " newcount "/ " (count urls-coll))
                                  (if (= (count urls-coll) newcount)
                                    (deliver results (map deref initial))
                                    (doall (take (+ newcount initial-count) initial)))))
    (deref results)))
    
(defn crawl-urls
  "Takes a collection of urls and tries to pull information from their HTML source. Takes an optional
  :opt param with values :phone, :email, or nil. An optional :concurrent-ops param indicates how many
  simultaneous network requests to make"
  [urls-coll & {:keys [opt concurrent-ops] :or {concurrent-ops 3}}]
  (let [urls-info (request-webpage-source urls-coll)]
    (condp = opt
      :phone (reduce (fn [results {{:keys [url html]} :info}]
                       (assoc results url {:phone (apply str (get-phone-number html))}))
                     (hash-map)
                     urls-info)
      :email (reduce (fn [results {{:keys [url html]} :info}]
                       (assoc results url {:email (get-email-address html)}))
                       (hash-map)
                       urls-info)
      (reduce (fn [results {{:keys [url html]} :info}]
                        (assoc results url {:email (get-email-address html)
                                            :phone (get-phone-number html)}))
                      (hash-map)
                      urls-info))))


(defn count-pos [results opt] (count (filter (fn [[_ {v opt}]] (not (nil? v))) results)))

#_(defn- summarize-leads
  [leads-json out-filename & {:keys [outdir] :or {outdir (. System getProperty "user.home")}}]
  (let [leads-map (reduce (fn [result {:strs [url category state city]}]
                            (-> result
                                (assoc url {:category category :state state :city city})
                                (update-in [:urls] conj url)))
                          (identity {:urls []})
                          leads-json)
        urls (:urls leads-map)
        results (get-emails urls)
        filtered-results (reduce (fn [acc [url crawl-result]]
                                   (if (nil? (:email crawl-result))
                                     (identity acc)
                                     (assoc acc url (merge (leads-map url) crawl-result))))
                                 (hash-map)
                                 results)
        outfile (doto (java.io.File. (str outdir (:separator config) out-filename ".csv"))
                  (.createNewFile))]
    (with-open [out (java.io.FileWriter. outfile)]
      (doseq [[url {:keys [category state city email]}] filtered-results]
        (.write out (str category "," state "," city "," url "," email "\n"))))))
