(ns clj-dbx.sql-builder
  (:require [clojure.string :as str]))

(defprotocol ToSql
  (-to-sql [this]))

(extend-protocol ToSql
  Long
  (-to-sql [this] this)
  String
  (-to-sql [this] (str "'" (str/replace this #"\'" "''") "'")))

(defn build-command [parsed-template param-map]
  (str/join (map #(cond (string? %) %
                        (keyword? %) (if-let [value (get param-map %)]
                                       (-to-sql value)))
                 parsed-template)))
