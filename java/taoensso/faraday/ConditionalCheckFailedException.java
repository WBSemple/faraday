package taoensso.faraday;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;

public class ConditionalCheckFailedException extends ExceptionInfo {
    public ConditionalCheckFailedException(String s, IPersistentMap data) {
        super(s, data);
    }
}
