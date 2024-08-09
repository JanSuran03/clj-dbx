(ns clj-dbx.result-set-reader
  (:import (java.sql ResultSet)))

(defn transform [val]
  (if-let [f ({java.sql.Date #(java.util.Date/from (.toInstant ^java.sql.Date %))
               Integer       long} (class val))]
    (f val)
    val))

(defn read-result-set [^ResultSet rs]
  (let [rs-meta (.getMetaData rs)
        column-count (.getColumnCount rs-meta)
        _ (.getColumnType rs-meta 1)
        column-indices (doall (range 1 (inc column-count)))
        column-names (mapv #(keyword (.getColumnName rs-meta %)) column-indices)
        column-name-kws (mapv keyword column-names)]
    (loop [res (transient [])]
      (if (.next rs)
        (recur (conj! res (persistent!
                            (reduce (fn [m ^Integer i]
                                      (if-let [val (.getObject rs i)]
                                        (assoc! m (column-name-kws (dec i)) (transform val))
                                        m))
                                    (transient {})
                                    column-indices))))
        (persistent! res)))))
