(ns leadscore.constants
  (:import (java.io File)))

(def separator (. File separator))
(def user-dir (. System getProperty "user.dir"))
(def user-home (. System getProperty "user.home"))
(def user-downloads-dir (str user-home separator "Downloads"))
(def reports-dir (str user-dir separator "reports"))
(def resources-dir (str user-dir separator "resources"))
(def buffers-dir (str resources-dir separator "buffers"))
(def phone-matcher #"\(?\d{3}\)?[\s\.-]\d{3}[\s\.-]\d{4}")
(def asummed-user-agent "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:73.0) Gecko/20100101 Firefox/73.0")