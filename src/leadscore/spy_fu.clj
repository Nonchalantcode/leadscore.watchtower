(ns leadscore.spy-fu
  (:require [leadscore.netcore :as leads :refer (read-page-source)]
            [cheshire.core :as JSON]))

(set! *warn-on-reflection* true)

(def ^:const ^:private spyfu-domain "spyfu.com")

(defn query-api
  "Talks to Spy-Fu's Lead API. If :parse-json is true, returns a JSON response from the API."
  [api-key domain & {:keys [parse-json] :or {parse-json true}}]
  (let [spyfu-url (str "https://www.spyfu.com/apis/leads_api/get_contact_card?domain="
                       domain "&api_key=" api-key)
        response (leads/read-page-source spyfu-url)]
    (if parse-json (JSON/decode response) response)))

(defn valid-apiKey? [api-key]
  (let [spyfu-url (str "https://www.spyfu.com/apis/leads_api/get_contact_card?domain="
                       spyfu-domain "&api_key=" api-key)
        connection (.openConnection (java.net.URL. spyfu-url))
        [version status-code reason] (.split (.getHeaderField connection nil) "\\s")]
    (if (= 200 (Integer/valueOf ^String (identity status-code))) true false)))

(defn await-batch
  "Takes a collection of urls to query and waits for all of them to finish querying the spyfu leads API"
  [api-key urls-coll]
  (let [counter (atom 0) p (promise) values (atom {})]
    (add-watch counter :inc (fn [k ref old-value new-value]
                              (if (= new-value (count urls-coll))
                                (deliver p @values))))
    (doall (map (fn [curr] (future
                             (swap! values assoc curr (query-api api-key curr :parse-json true))
                             (swap! counter inc)))
                urls-coll))
    @p))
