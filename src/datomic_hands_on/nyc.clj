(ns datomic-hands-on.nyc
  (:use [datomic.api :only [q db] :as d]))



(defn tempid
  []
  (d/tempid :db.part/user))

(def uri "datomic:mem://localhost:4334/lispmeetup")

(d/create-database uri)


(def conn (d/connect uri))


(def schema {:db/id (d/tempid :db.part/db)
             :db/ident :email
             :db/valueType :db.type/string
             :db/cardinality :db.cardinality/one
             :db/unique :db.unique/identity
             :db.install/_attribute :db.part/db})


(def schema-ret @(d/transact conn [schema]))

(keys schema-ret)

;; newdb is the new db returned by the transaction
(def newdb (:db-after schema-ret))


;; Equality on two DBs !
;; is newdb equals to previous DB
(= newdb (:db-defore schema-ret))


;; If nobody else uses the DB is newdb equals to current DB ?
(= newdb (d/db conn))



(def query '[:find ?e ?email
             :where [?e :email ?email]])


;; We don't have any data yet
(d/q query newdb)



;__________________________________________________________
;                                                          |
;             Let's add Fred's email                       |
;__________________________________________________________|

(def fred-ret @(d/transact conn [{:db/id (tempid)
                                  :email "fred@email.com"}]))

(keys fred-ret)

;; map from tempIds to Ids
(:tempids fred-ret)

;; function to get sole id of the transaction
(def an-id #(-> % :tempids first val))


(def fred-id (-> fred-ret :tempids first val))

(identity fred-id)


;; let's keep the value of the DB after adding Fred's email
(def fred-db (:db-after fred-ret))

(d/q query fred-db)


;__________________________________________________________
;                                                          |
;             Let's add Ethel's email                      |
;__________________________________________________________|

(def ethel-ret @(d/transact conn [{:db/id (tempid)
                                   :email "ethel@email.com"}]))

(def ethel-db (:db-after ethel-ret))

(d/q query ethel-db)



;__________________________________________________________
;                                                          |
;             Speculative changes                          |
;__________________________________________________________|


; Fred changes name to freddy
(def freddy-tx [{:db/id (tempid)
                :email "freddy@email.com"}])


(def freddy-db (-> (d/db conn)
                   (d/with freddy-tx)
                   :db-after))


(d/q query freddy-db) ; Oups

; We should use Fred's id not new id
(def freddy-tx [{:db/id fred-id
                 :email "freddy@email.com"}])


(def freddy-db (-> (d/db conn)
                   (d/with freddy-tx)
                   :db-after))

(d/q query freddy-db)


;__________________________________________________________
;                                                          |
;             Now transact for real                        |
;__________________________________________________________|


(def freddy-db (-> conn
                   (d/transact freddy-tx)
                   deref
                   :db-after  ))


(d/q query freddy-db)


(d/q query (d/db conn))


;__________________________________________________________
;                                                          |
;             An information system retains history        |
;__________________________________________________________|


(def latest-db (d/db conn))

;;; enhance query to get txes
(def tquery '[:find ?e ?email ?tx
             :where [?e :email ?email ?tx]] )

(d/q query (d/history latest-db)) ;; hrm, more results, why ?


(d/q tquery (d/history latest-db)) ;; hrm, more results, why ?




;;;; get assertion/retraction status too
(def full-query '[:find ?e ?email ?tx ?added
                  :where [?e :email ?email ?tx ?added]])


(d/q full-query (d/history latest-db))


(d/q '[:find ?e
       :where [?e :email "freddy@email.com"]] latest-db)


(d/q
  '[:find ?diff
    :in $a $b
    :where
    [$a ?gp :parent ?f]
    [$a ?f :parent ?pf]
    [$b ?gp :age ?gpa]
    [$b ?pf :age ?pfa]
    [(- ?gpa ?pfa) ?diff]]
  [[:pierrette :parent :muriel]
   [:muriel :parent :louis]]
  [[:pierrette :age 65]
   [:louis :age 5]
   [:muriel :age 47]])



(d/q
  '[:find ?sk ?var
    :in $a $b
    :where
    [$a ?sk :price ?pa]
    [$b ?sk :price ?pb]
    [(- ?pb ?pa) ?var]]
  [[:sk1 :price 12.90M]
  [:sk2 :price 13.89M]
  [:sk3 :price 25M]]
 [[:sk1 :price 12.93M]
  [:sk2 :price 13.67M]
  [:sk3 :price 27M]])


