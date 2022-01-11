package io.appform.databuilderframework.engine;


import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

import java.util.Collections;
import java.util.Map;

/**
 * Created by ajaysingh on 13/06/14.
 */
@Value
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DataBuilderException extends Exception {
    public enum ErrorCode {
        HANDLER_FAILURE
    }

    ErrorCode errorCode;
    Map<String, Object> details;


    public DataBuilderException(String message) {
        this(ErrorCode.HANDLER_FAILURE, message);
    }

    public DataBuilderException(ErrorCode errorCode, String message) {
        this(errorCode, message, Collections.emptyMap());
    }

    public DataBuilderException(ErrorCode errorCode, String message, Throwable cause) {
        this(errorCode, message, Collections.emptyMap(), cause);
    }

    public DataBuilderException(ErrorCode errorCode, String message, Map<String, Object> details) {
        this(errorCode, message, details, null);
    }

    public DataBuilderException(ErrorCode errorCode, String message, Map<String, Object> details, Throwable cause) {
        super(message, cause);
        this.details = details;
        this.errorCode = errorCode;
    }
}
