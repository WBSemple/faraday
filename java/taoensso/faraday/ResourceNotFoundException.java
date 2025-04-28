package taoensso.faraday;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;

public class ResourceNotFoundException extends ExceptionInfo {
    public ResourceNotFoundException(String s, IPersistentMap data) {
        super(s, data);
    }
}
