package taoensso.faraday;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;

public class ResourceInUseException extends ExceptionInfo {
    public ResourceInUseException(String s, IPersistentMap data) {
        super(s, data);
    }
}
