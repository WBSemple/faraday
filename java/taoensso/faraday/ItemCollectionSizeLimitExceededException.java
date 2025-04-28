package taoensso.faraday;

import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;

public class ItemCollectionSizeLimitExceededException extends ExceptionInfo {
    public ItemCollectionSizeLimitExceededException(String s, IPersistentMap data) {
        super(s, data);
    }
}
