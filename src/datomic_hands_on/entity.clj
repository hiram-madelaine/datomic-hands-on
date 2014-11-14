(ns datomic-hands-on.entity
  (:require [datomic.api :refer [q db] :as d]
            [clojure.pprint :refer [pprint] :as pp]))


;; Dispplay all entities
(->> (d/q '[:find ?e ?ident
            :where
            [?e :db/ident ?ident]]
          (d/db conn))
     (into (sorted-map))
     pprint)