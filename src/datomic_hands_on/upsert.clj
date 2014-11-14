(ns datomic-hands-on.upsert
  (:require [datomic.api :refer [q db] :as d]
            [clojure.pprint :refer [pprint] :as pp]))

(defn tempid
  []
  (d/tempid :db.part/user))

(def uri "datomic:mem://localhost:4334/upsert")

#_(def uri "datomic:free://localhost:4334/upsert")


(d/create-database uri)


(def conn (d/connect uri))


(def schema [{:db/id                 (d/tempid :db.part/db)
              :db/ident              :price
              :db/valueType          :db.type/bigdec
              :db/cardinality        :db.cardinality/one
              :db.install/_attribute :db.part/db}
             {:db/id                 (d/tempid :db.part/db)
              :db/ident              :serial
              :db/valueType          :db.type/keyword
              :db/cardinality        :db.cardinality/one
              :db.install/_attribute :db.part/db}])


(def schema-ret @(d/transact conn schema))



(def query '[:find ?sn ?price
             :where
             [?e :price ?price]
             [?e :serial ?sn]])


;__________________________________________________________
;                                                          |
;             Let's add :sk1's price                       |
;__________________________________________________________|

(d/transact conn [{:db/id  (tempid)
                   :serial :sk1
                   :price  10.55M}])



(d/q query (d/db conn))

;__________________________________________________________
;                                                          |
;             Speculative changes                          |
;__________________________________________________________|


; Change price of sk1 item
(def sk1-price-tx [{:db/id (tempid)
                    :serial :sk1
                    :price 15.00M}])


(def sk1-price2-db (-> (d/db conn)
                       (d/with sk1-price-tx)
                       :db-after))


(d/q query sk1-price2-db) ;;oups


(d/q '[:find ?e
       :where [?e  :db/ident :serial]] (d/db conn))


;; Alering the schema with index true
(d/transact conn [{:db/id :serial
                   :db/index true
                   :db.alter/_attribute :db.part/db}])

(d/transact conn [{:db/id :serial
                   :db/unique :db.unique/identity
                   :db.alter/_attribute :db.part/db}])

(def sk1-price2-db (-> (d/db conn)
                       (d/with sk1-price-tx)
                       :db-after))


;; Although we used a tempid in the tx, with :db/unique :db.unique/identity id does not create a second item.
(d/q query sk1-price2-db)


;__________________________________________________________
;                                                          |
;             Now we can transact for real                 |
;__________________________________________________________|


(def third-db (-> conn
                  (d/transact sk1-price-tx)
                  deref
                  :db-after))


(d/q query third-db)


(d/q query (d/history  (d/db conn)))

