(ns leadscore.spy-fu
  (:require [leadscore.netcore :as leads :refer (read-page-source)]
            [cheshire.core :as JSON]))

(set! *warn-on-reflection* true)

