package taoensso.faraday;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;

public class ValidationException extends ExceptionInfo {
    public ValidationException(String s, IPersistentMap data) {
        super(s, data);
    }
}
