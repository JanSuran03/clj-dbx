(ns clj-dbx.core
  (:require [clj-dbx.dsl-parser :as parser]
            [clj-dbx.sql-builder :as builder]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import (java.sql Connection DriverManager ResultSet)))

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

(defn -main []
  (let [{{:user/keys [username password]} :user
         {db-name :database/name}         :database} (edn/read-string (slurp "config.edn"))
        conn-config #:connection{:username username :password password :db-name db-name}]
    ;(create-database conn-config)
    (with-db-connection conn-config
      (fn [^Connection conn]
        (with-open [stmt (.createStatement conn)]
          (.execute stmt (lines "DROP TABLE IF EXISTS employees"))
          (.execute stmt (lines "CREATE TABLE IF NOT EXISTS employees"
                                "("
                                "  id SERIAL PRIMARY KEY,"
                                "  name VARCHAR(120) NOT NULL,"
                                "  age INTEGER NOT NULL,"
                                "  salary INTEGER NOT NULL,"
                                "  num_children INTEGER NOT NULL"
                                ")"))
          (.execute stmt (lines "INSERT INTO employees (name, age, salary, num_children)"
                                "VALUES ('John Doe', 30, 60000, 2)"))
          (.execute stmt (lines "INSERT INTO employees (name, age, salary, num_children)"
                                "VALUES ('Joe Doe', 45, 70000, 3)"))
          (.execute stmt (lines "INSERT INTO employees (name, age, salary, num_children)"
                                "VALUES ('John Smith', 40, 55000, 1 )"))
          (.execute stmt (lines "INSERT INTO employees (name, age, salary, num_children)"
                                "VALUES ('Joe O''Neill', 50, 65000, 3 )"))
          (.executeQuery stmt "SELECT * FROM employees")
          (let [yield (fn [^ResultSet rs]
                        ((fn -yield []
                           (when (.next rs)
                             (cons {:id           (.getInt rs "id")
                                    :name         (.getString rs "name")
                                    :age          (.getInt rs "age")
                                    :salary       (.getInt rs "salary")
                                    :num-children (.getInt rs "num_children")}
                                   (-yield))))))]
            (assert (= (yield (.getResultSet stmt))
                       [{:id 1, :name "John Doe", :age 30, :salary 60000, :num-children 2}
                        {:id 2, :name "Joe Doe", :age 45, :salary 70000, :num-children 3}
                        {:id 3, :name "John Smith", :age 40, :salary 55000, :num-children 1}
                        {:id 4, :name "Joe O'Neill", :age 50, :salary 65000, :num-children 3}]))
            (.executeQuery stmt (builder/build-command (parser/parse-query "SELECT * FROM employees WHERE id = :id OR num_children = :num-children")
                                                       {:id 1 :num-children 3}))
            (assert (= (yield (.getResultSet stmt))
                       [{:id 1, :name "John Doe", :age 30, :salary 60000, :num-children 2}
                        {:id 2, :name "Joe Doe", :age 45, :salary 70000, :num-children 3}
                        {:id 4, :name "Joe O'Neill", :age 50, :salary 65000, :num-children 3}]))))))))
