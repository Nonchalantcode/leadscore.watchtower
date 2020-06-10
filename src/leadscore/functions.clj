(ns leadscore.functions
  (:import java.util.function.Consumer
           (java.util HashSet LinkedList)
           (java.io File FileReader BufferedReader InputStreamReader)))

(defn noop [])

(defn get-hostname [^String url]
  (last (re-find #"(http:\/\/|https:\/\/)?(www\.)?(.+)\.\w+$" url)))

(defmacro iterate! [s bindings & forms]
  `(.forEachRemaining (.iterator ~s)
                      (reify Consumer
                        (~'accept [~'this v#]
                          (let [~bindings v#]
                            ~@forms)))))

(defmacro thread-if
  "Takes a list of conditional expressions and evaluates them. Then, evaluates the rest of the
   forms given as a forms common to the block after the conditional block"
  [conditional-forms & rest-forms]
  (list* 'do (map (fn [condition-expr]
                    (let [[conditional expr] condition-expr]
                      `(if ~conditional ~(list* 'do expr rest-forms) nil)))
                  (partition 2 2 conditional-forms))))

(defmacro thread-cond
  "Takes a list of conditional expressions and evaluates them. Then, evaluates the rest of the
   forms given as a forms common to the block after the conditional block"
  [conditional-forms & rest-forms]
  (loop [iteration 1 forms conditional-forms results nil]
    (if (= 1 iteration)
      (recur (inc iteration) (next forms) (list 'cond (first forms)))
      (if (= 2 iteration)
        (recur (inc iteration) (next forms) (conj (vec results) (list* 'do (first forms) rest-forms)))
        (if (nil? forms)
          (apply list results)
          (if (odd? iteration)
            (recur (inc iteration) (next forms) (conj results (first forms)))
            (recur (inc iteration) (next forms) (conj results (list* 'do (first forms) rest-forms)))))))))

(derive java.io.FileReader ::readable)
(derive java.io.InputStreamReader ::readable)

(defmulti for-each
  "Iterates over the entries of a text stream represented by a file path string,
   a java.io.File, or any instance of java.io.InputStream which can be interpreted as a character stream"
  (fn [handle callback]
    (type handle))
  :default java.lang.String)

(defmethod for-each java.lang.String
  [handle callback]
  (for-each (File. handle) callback))

(defmethod for-each java.io.File
  [handle callback]
  (for-each (FileReader. handle) callback))

(defmethod for-each java.io.InputStream
  [handle callback]
  (for-each (InputStreamReader. handle) callback))

(defmethod for-each ::readable
  [handle callback]
  (with-open [contents (-> handle (BufferedReader.) (.lines))]
    (.forEach contents (reify Consumer
                         (accept [this v] (callback v))))))

(defmethod for-each java.util.stream.Stream
  [handle callback]
  (.forEach handle (reify Consumer
                     (accept [this v] (callback v)))))

(defn load-config [config-file]
  (let [conf (StringBuilder.)]
    (for-each config-file #(.append conf ^CharSequence %))
    (read-string (.toString conf))))

(defn load-veto-lists [& sources]
  (let [veto-list (HashSet.)]
    (doseq [source sources]
      (for-each source #(.add veto-list %)))
    veto-list))

(defn async-call
  "Runs a call of the function on :func in a future. At all times there will be n futures running where n
   corresponds to the size arg"
  [size coll & {:keys [func]}]
  (let [init (take size coll)
        results (atom [])
        remaining (LinkedList. (drop size coll))
        p (promise)]
    (add-watch results :tally (fn [key reference old new]
                                (cond
                                  (= (count coll) (count new)) (deliver p new)
                                  (zero? (.size remaining)) (noop)
                                  :else (future (swap! results conj (func (.pop remaining)))))))
    (doseq [arg init]
      (future (swap! results conj (func arg))))
    (deref p)))