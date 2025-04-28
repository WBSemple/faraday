package taoensso.faraday;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;

public class InternalServerErrorException extends ExceptionInfo {
    public InternalServerErrorException(String s, IPersistentMap data) {
        super(s, data);
    }
}
