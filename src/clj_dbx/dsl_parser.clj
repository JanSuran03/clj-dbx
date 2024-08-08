(ns clj-dbx.dsl-parser)

(defn- kw-char? [ch] (or (#{\- \_} ch) (Character/isAlphabetic (int ch))))

(defmulti ^:private -parse-query (fn [m _] (:state m)))

(defmethod -parse-query :state/read-sql
  [{:keys [token-buf token-seq] :as m} ch]
  (if (identical? ch \:)
    {:state     :state/read-kw
     :token-seq (cond-> token-seq (seq token-buf) (conj token-buf))}
    (update m :token-buf str ch)))

(defmethod -parse-query :state/read-kw
  [{:keys [token-seq token-buf] :as m} ch]
  (cond (identical? ch \*)
        (if (seq token-buf)
          (throw (ex-info "Array kw decl must be right after the leading colon" {:processed token-seq :kw-buf token-buf}))
          (assoc m :state :state/read-kw-arr))

        (kw-char? ch)
        (update m :token-buf str ch)

        (seq token-buf)
        {:token-seq (conj token-seq (keyword token-buf))
         :state     :state/read-sql
         :token-buf (str ch)}

        :else
        (throw (ex-info "Empty keyword buffer" {:processed token-seq}))))

(defn parse-query [query-str]
  (let [{:keys [token-buf token-seq]} (reduce -parse-query
                                              {:state :state/read-sql :token-seq []}
                                              (str query-str " "))
        token-buf (subs token-buf 0 (dec (count token-buf)))]
    (cond-> token-seq (seq token-buf) (conj token-buf))))

