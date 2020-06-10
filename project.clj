(defproject leadscore.watchtower "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [cheshire "5.10.0"]
                 [ring "1.7.0"]
                 [compojure "1.6.1"]
                 [ring/ring-mock "0.4.0"]
                 [enlive "1.1.6"]
                 [mysql/mysql-connector-java "8.0.20"]
                 [org.clojure/java.jdbc "0.7.11"]]
  :main ^:skip-aot leadscore.watchtower
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
