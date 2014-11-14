(ns datomic-hands-on.prices
  (:require [datomic.api :refer [q db] :as d]
            [clojure.pprint :refer [pprint] :as pp]))



(defn tempid
  []
  (d/tempid :db.part/user))

(def uri "datomic:mem://localhost:4334/prices")

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
              ;:db/unique :db.unique/identity
              :db.install/_attribute :db.part/db}])


(def schema-ret @(d/transact conn schema))

(keys schema-ret)

(:tempids schema-ret)
(:tx-data schema-ret)


;; newdb is the new db returned by the transaction
(def newdb (:db-after schema-ret))


;; Equality on two DBs !
;; is newdb equals to DB before transaction ?
(= newdb (:db-defore schema-ret))


;; If nobody else uses the DB, is newdb equals to current DB ?
(= newdb (d/db conn))



(def query '[:find ?sn ?price
             :where
             [?e :price ?price]
             [?e :serial ?sn]])


;; We don't have any data yet
(d/q query newdb)


;__________________________________________________________
;                                                          |
;             Let's add :sk1's price                       |
;__________________________________________________________|

(def first-ret @(d/transact conn [{:db/id (tempid)
                                  :serial :sk1
                                  :price 10.55M}]))

(keys first-ret)

;; map from tempIds to Ids
(:tempids first-ret)

;; function to get sole id of the transaction
(def an-id #(-> % :tempids first val))


(def sk1-id (-> first-ret :tempids first val))

(identity sk1-id)


;; let's keep the value of the DB after adding sk1's price
(def initial-db (:db-after first-ret))


(d/q query initial-db)

;__________________________________________________________
;                                                          |
;             Let's add SK2's price                        |
;__________________________________________________________|

(def second-ret @(d/transact conn [{:db/id (tempid)
                                   :price 13.89M
                                   :serial :sk2}]))

(def second-db (:db-after second-ret))

(d/q query second-db)


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


(d/q query sk1-price2-db) ; Oups

; We should use sk1's id not a new id
(def sk1-price-tx [{:db/id sk1-id
                    :price 15.00M}])


(def spec-db (-> (d/db conn)
                 (d/with sk1-price-tx)
                 :db-after))

(d/q query spec-db)


;__________________________________________________________
;                                                          |
;             Now we can transact for real                 |
;__________________________________________________________|


(def third-db (-> conn
                   (d/transact sk1-price-tx)
                   deref
                   :db-after))


(d/q query third-db)


(d/q query (d/db conn))


;__________________________________________________________
;                                                          |
;             An information system retains history        |
;__________________________________________________________|


(def latest-db (d/db conn))

;;; enhance query to get txes
(def tquery '[:find ?sn ?price ?tx
             :where
              [?e :price ?price ?tx]
              [?e :serial ?sn]] )

(d/q query (d/history latest-db)) ;; hrm, more results, why ?


(d/q tquery (d/history latest-db)) ;; hrm, more results, why ?


;;;; get assertion/retraction status too
(def full-query '[:find ?sn ?price ?tx ?added
                  :where
                  [?e :price ?price ?tx ?added]
                  [?e :serial ?sn]])


(d/q full-query (d/history latest-db))


(d/q '[:find ?p
       :where
       [?e :serial  :sk1]
       [?e :price ?p]] latest-db)



;__________________________________________________________
;                                                          |
;             Query two DBs                                |
;__________________________________________________________|

;; price difference/serial since last tx

(d/q
  '[:find ?sn ?var
    :in $a $b
    :where
    [$a ?e :price ?pa]
    [$a ?e :serial ?sn]
    [$b ?e :price ?pb]
    [(- ?pb ?pa) ?var]]
  second-db
  latest-db)

; max price variation
(d/q
  '[:find (max ?var)
    :in $a $b
    :where
    [$a ?e :price ?pa]
    [$a ?e :serial ?sn]
    [$b ?e :price ?pb]
    [(- ?pb ?pa) ?var]]
  second-db
  latest-db)



;; Filter only items with variation
(d/q
  '[:find  ?var
    :in $a $b
    :where
    [$a ?e :price ?pa]
    [$a ?e :serial ?sn]
    [$b ?e :price ?pb]
    [(- ?pb ?pa) ?var]
    [(not= 0.00M ?var)]]
  second-db
  latest-db)

