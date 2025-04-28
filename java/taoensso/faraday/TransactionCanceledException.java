package taoensso.faraday;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;

public class TransactionCanceledException extends ExceptionInfo {
    public TransactionCanceledException(String s, IPersistentMap data) {
        super(s, data);
    }
}
