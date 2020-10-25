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
