(set! *warn-on-reflection* true)
(ns clj-dbx.core
  (:require [clj-dbx.dsl-parser :as parser]
            [clj-dbx.result-set-reader :as rs-reader]
            [clj-dbx.sql-builder :as builder]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as str])
  (:import (java.sql Connection DriverManager)
           (java.util Date)))

(defn lines [& lines]
  (str/join "\n" lines))

(defn with-db-connection [{:connection/keys [username password db-name]} f]
  (with-open [conn (DriverManager/getConnection (str "jdbc:postgresql://localhost:5432/" db-name) username password)]
    (f conn)))

(defn create-database [{:connection/keys [username password db-name]}]
  (with-open [conn (DriverManager/getConnection "jdbc:postgresql://localhost:5432/postgres" username password)
              stmt (.createStatement conn)]
    (.executeUpdate stmt (str "CREATE DATABASE " db-name " OWNER " username))
    (println "Database created successfully")))

(defn rand-time []
  (Date. (long (rand (.getTime (Date.))))))

(defn inbounds-rand-nth [coll index]
  (nth coll (rem (Math/abs ^long index) (count coll))))

(defn -main []
  (let [{{:user/keys [username password]} :user
         {db-name :database/name}         :database} (edn/read-string (slurp "config.edn"))
        conn-config #:connection{:username username :password password :db-name db-name}
        times (vec (repeatedly 10 rand-time))]
    ;(create-database conn-config)
    (with-db-connection conn-config
      (fn [^Connection conn]
        (with-open [stmt (.createStatement conn)]
          (.execute stmt (lines "DROP TABLE IF EXISTS employees"))
          (.execute stmt (lines "CREATE TABLE IF NOT EXISTS employees"
                                "("
                                "  id UUID PRIMARY KEY,"
                                "  name VARCHAR(120) NOT NULL,"
                                "  age INTEGER NOT NULL,"
                                "  salary INTEGER NOT NULL,"
                                "  num_children INTEGER NOT NULL,"
                                "  registered_at TIMESTAMP NOT NULL"
                                ")"))
          (let [prep-sql (parser/parse-query (lines "INSERT INTO employees"
                                                    "(id, name, age, salary, num_children, registered_at) VALUES"
                                                    "(:id, :name, :age, :salary, :num-children, :registered-at)"))
                employees (->> [{:name "John Doe" :age 30 :salary 60000 :num-children 2}
                                {:name "Joe Doe" :age 45 :salary 70000 :num-children 3}
                                {:name "John Smith" :age 40 :salary 55000 :num-children 1}
                                {:name "Joe O'Neill" :age 50 :salary 65000 :num-children 3}]
                               (map #(assoc % :registered-at (inbounds-rand-nth times (hash (:name %)))
                                              :id (random-uuid))))
                seq-apply-to-instant (fn [s] (map (fn [m] (update m :registered-at #(.toInstant ^Date %))) s))]
            (doseq [employee employees]
              (.execute stmt (builder/build-command prep-sql employee)))
            (.executeQuery stmt "SELECT * FROM employees")
            (assert (= (set (seq-apply-to-instant employees))
                       (set (seq-apply-to-instant (map #(set/rename-keys % {:num_children  :num-children
                                                                            :registered_at :registered-at})
                                                       (rs-reader/read-result-set (.getResultSet stmt)))))))))))))
