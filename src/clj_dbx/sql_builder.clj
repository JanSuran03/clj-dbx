(ns clj-dbx.sql-builder
  (:require [clojure.string :as str])
  (:import (clojure.lang ISeq Seqable)
           (java.text SimpleDateFormat)
           (java.time ZonedDateTime)
           (java.time.format DateTimeFormatter)
           (java.util Date Iterator UUID)))

(defprotocol ToSql
  (-to-sql [this]))

(def ^:private zdt-formatter (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSSSSSSXXX"))
(def ^:private date-formatter (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss.SSS"))

(defn- seq-to-sql [s] (str "(" (str/join "," (map -to-sql s)) ")"))

(extend-protocol ToSql
  nil
  (-to-sql [_] "NULL")
  Long
  (-to-sql [this] this)
  Double
  (-to-sql [this] this)
  String
  (-to-sql [this] (str "'" (str/replace this #"\'" "''") "'"))
  UUID
  (-to-sql [this] (str "'" this "'"))
  Boolean
  (-to-sql [this] (if this "TRUE" "FALSE"))
  ISeq
  (-to-sql [this] (seq-to-sql this))
  Seqable
  (-to-sql [this] (seq-to-sql this))
  Iterable
  (-to-sql [this] (seq-to-sql this))
  Iterator
  (-to-sql [this] (-to-sql (iterator-seq this)))
  ZonedDateTime
  (-to-sql [this] (str "'" (.format this zdt-formatter) "'"))
  Date
  (-to-sql [this] (str "'" (.format date-formatter this) "'")))

(defn to-sql [^Object x]
  (if (-> x .getClass .isArray)
    (seq-to-sql x)
    (-to-sql x)))

(defn build-command [parsed-template param-map]
  (str/join (map #(cond (string? %) %
                        (keyword? %) (if-let [value (get param-map %)]
                                       (to-sql value)
                                       (throw (ex-info "Keyword param not present" {:param-name %
                                                                                    :param-map  param-map}))))
                 parsed-template)))
