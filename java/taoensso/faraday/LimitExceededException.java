package taoensso.faraday;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;

public class LimitExceededException extends ExceptionInfo {
    public LimitExceededException(String s, IPersistentMap data) {
        super(s, data);
    }
}
