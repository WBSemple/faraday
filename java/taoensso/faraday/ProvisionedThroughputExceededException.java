package taoensso.faraday;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;

public class ProvisionedThroughputExceededException extends ExceptionInfo {
    public ProvisionedThroughputExceededException(String s, IPersistentMap data) {
        super(s, data);
    }
}
