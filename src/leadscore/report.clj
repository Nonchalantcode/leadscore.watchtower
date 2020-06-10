(ns leadscore.report
  (:require (clojure.java [io :as io])
            (leadscore [constants :refer (reports-dir)]))
  (:import [java.io File InputStream]))


(defn read-contents ^String [^InputStream contents]
  (with-open [reader (io/reader contents)]
    (loop [buffer (StringBuffer. (* 1024 10))]
      (let [line (.readLine reader)]
        (if (nil? line)
          (.toString buffer)
          (recur (doto buffer (.append line) (.append "\n"))))))))

(defn- create-report-file
  [^String content & {:keys [storage-location type] :or {storage-location (doto (File. reports-dir) .mkdir) type "ALL-READS"}}]
  (let [file (File. storage-location (str (gensym type) ".csv"))
        writer (io/writer file)]
    (.write writer content)
    (.close writer)
    file))

(defn generate-report [^InputStream contents & {:keys [storage-location type] :or {storage-location (doto (File. reports-dir) .mkdir) type "ALL-READS"}}]
  (-> contents read-contents (create-report-file :storage-location storage-location :type type)))