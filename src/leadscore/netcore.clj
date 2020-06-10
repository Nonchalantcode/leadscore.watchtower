(ns leadscore.netcore
  (:import [java.net URL HttpURLConnection MalformedURLException]
           [java.io InputStream
            InputStreamReader
            IOException]
           [java.util Date
            LinkedList])
  (:require (clojure [string :refer (replace-first starts-with?)])
            (leadscore [constants :as constants :refer (phone-matcher asummed-user-agent)]
                       [report])))

(set! *warn-on-reflection* true)

(defmacro try-match [str-source & regexes]
  `(or ~@(map (fn [reg] `(re-find ~reg ~str-source)) regexes)))

(defn query-url
  "Open an InputStream on an instance of java.net.URL"
  [^String url]
  (try
    (-> url (URL.) (.openStream))
    (catch MalformedURLException e
      (println "Malformed URL. Probably missing its protocol."))
    (catch IOException e
      (println (.getMessage e)))))

(defn read-as-text-stream
  "Reads the contents of an InputStream by wrapping it in an InputStreamReader"
  [^InputStream in ^Integer buffsize arrsize]
  (with-open [source (InputStreamReader. in)]
    (loop [contents (StringBuffer. buffsize) arr (char-array arrsize)]
      (let [n (.read source arr 0 arrsize)]
        (if (not= -1 n)
          (recur (.append contents (.trim (String. arr))) (char-array arrsize))
          (.toString contents))))))

(defn read-url-source
  ([^InputStream url-stream]
   (read-as-text-stream url-stream 2014 256))
  ([^InputStream url-stream buffsize arrsize]
   (read-as-text-stream url-stream buffsize arrsize)))

(defn isCanonicalUrl [^String uri]
  (try
    (if (URL. uri) true)
    (catch MalformedURLException e false)))

(defn toCanonicalUrl
  "Asumming the hostname is present in URL and the only thing needed to make it canonical is the protocol"
  [^String url]
  (cond
    (starts-with? url "http") url
    :else (str "http://" url)))

(defn toCanonicalSecureUrl
  [^String url]
  (cond
    (starts-with? url "https") url
    (starts-with? url "http") (replace-first url "http" "https")
    :else (str "https://" url)))

(defn- are-same-numbers [& numbers]
  (= (map #(apply str (re-seq #"\d+" %)) numbers)))

(defn is-false-number [^String number-str & patterns]
  (some #(.startsWith number-str %) patterns))

(defn- first-two-results [phone-urls-coll]
  (let [first-two (take 2 (set (filter #(not (is-false-number % "000" "(999)")) phone-urls-coll)))]
    (if (apply are-same-numbers first-two)
      (take 1 first-two)
      (identity first-two))))

(defn- set-req-property [^HttpURLConnection connection prop val]
  (doto connection (.setRequestProperty prop val)))

(defn- open-connection ^HttpURLConnection [^String url & {:keys [secure?] :or {secure? false}}]
  (set-req-property
   (if secure?
     (-> url toCanonicalSecureUrl URL. .openConnection)
     (-> url toCanonicalUrl URL. .openConnection))
   "user-agent" asummed-user-agent))

(defn- get-stream ^InputStream [^HttpURLConnection connection]
  (.getInputStream connection))

(defn follow-redirect [^HttpURLConnection connection]
  (let [location (.getHeaderField connection "Location")]
    (if (starts-with? location "https")
      (open-connection location :secure? true)
      (open-connection location))))

(defn- read-from-connection
  [^HttpURLConnection connection & {:keys [buff1 buff2 callback]
                                    :or {buff1 (* 1024 4)
                                         buff2 (* 256 1)
                                         callback identity}}]
  (with-open [stream (get-stream connection)]
    (callback (read-as-text-stream stream buff1 buff2))))

(defn get-phone-match [^String source-str]
  (re-seq phone-matcher source-str))

(defn get-phone-number [host]
  (try
    (let [connection (open-connection host)
          status-code (.getResponseCode connection)
          follow-redirect (fn [^HttpURLConnection con]
                            (-> con
                                (follow-redirect)
                                (read-from-connection :callback get-phone-match)
                                (first-two-results)))]
      (cond
        (= 200 status-code)
        (-> connection (read-from-connection :callback get-phone-match) (first-two-results))

        (or (= 301 status-code) (= 302 status-code) (= 307 status-code) (= 308 status-code))
        (follow-redirect connection)

        (= 403 status-code)
        (throw (java.io.IOException. "Unable to read from this connection. 403"))

        (= 503 status-code)
        (throw (java.io.IOException. "Server responded with a 503 status code."))))
    (catch IOException ex
      (println "Host" [host] "failed with reason: " (.getMessage ex)))
    (catch Exception ex
      (println (.getMessage ex)))))

(defn- map-phone-info [^String url]
  (let [t1 (.getTime (Date.))]
    (-> (assoc {} url {:number (get-phone-number url)
                       :latency (- (.getTime (Date.)) t1)}))))

(defn- assoc-default-info [^String url default-info]
  (assoc {} url default-info))

(defn defer-crawl
  [url-collecttion]
  (doall (map (fn [url] [url (future (map-phone-info url))]) url-collecttion)))

(defn process-crawl-chunk
  [crawl-coll timeout]
  (let [partial-results (defer-crawl crawl-coll) sink (LinkedList.)]
    [(reduce (fn [acc [url results-map]]
               (merge acc (deref results-map timeout
                                 (do (.add sink [url results-map])
                                     {}))))
             (hash-map)
             partial-results) sink]))

(defn get-numbers
  [url-collection & {:keys [concurrent-requests timeout]
                     :or {concurrent-requests 6, timeout 2000}}]
  (let [chunks (partition concurrent-requests concurrent-requests nil url-collection)
        stuck-connections (LinkedList.)
        resolved-connections (reduce (fn [acc curr-chunk]
                                       (let [[processed-chunk unprocessed] (process-crawl-chunk curr-chunk timeout)]
                                         (.addAll stuck-connections unprocessed)
                                         (merge acc processed-chunk)))
                                     (hash-map)
                                     chunks)]
    (reduce (fn [acc [url f]]
              (merge acc (if (realized? f)
                           @f
                           (assoc-default-info url {:number nil, :latency 0}))))
            resolved-connections stuck-connections)))

(defn get-email-addr [^String url]
  (try
    (let [connection (doto (open-connection url) (.setConnectTimeout 10000) (.setReadTimeout 10000))
          status (.getResponseCode connection)
          page-source (cond
                        (= status 200) (read-from-connection connection)

                        (and (>= status 300) (<= status 308)) (-> (follow-redirect connection)
                                                                  (read-from-connection))

                        (>= status 400) nil)]

      (if (nil? page-source)
        nil
        #_(try-match page-source
                   ;; trivial case: email addresses inside <a> elements 
                   #"(?<=mailto:)[^\"]+"
                   #"[a-zA-Z0-9]+@[a-zA-Z0-9]+\.\w{2,4}"
                   ;; email addresses preceded by opening html tags, colons, or a space character
                   #"(?<=(>|\s|:))[\w\-]+@[\w\-]+\.\w{2,4}")
        (try-match page-source
                   #"[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+\.\w{2,4}")))
    (catch Exception ex
      (println "Host [" url "]" "has thrown an exception:\n\t" (.getMessage ex)))))

(defmacro ends-with [str-value & suffixes]
  `(or ~@(map (fn [v] `(. ~str-value ~'endsWith ~v)) suffixes)))
