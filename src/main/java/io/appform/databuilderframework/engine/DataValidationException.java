package io.appform.databuilderframework.engine;

import io.appform.databuilderframework.model.DataExecutionResponse;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

import java.util.Collections;
import java.util.Map;

/**
 * Created by kaustav.das on 26/03/15.
 */
@Value
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DataValidationException extends Exception {
    public enum ErrorCode {
        DATA_VALIDATION_EXCEPTION
    }

    ErrorCode errorCode;
    Map<String, Object> details;
    DataExecutionResponse response;



    public DataValidationException(String message) {
        this(ErrorCode.DATA_VALIDATION_EXCEPTION, message);
    }

    public DataValidationException(ErrorCode errorCode, String message) {
        this(errorCode, message, Collections.emptyMap());
    }

    public DataValidationException(ErrorCode errorCode, String message, Throwable cause) {
        this(errorCode, message, Collections.emptyMap(), cause);
    }

    public DataValidationException(ErrorCode errorCode, String message, Map<String, Object> details) {
        this(errorCode, message, details, null);
    }

    public DataValidationException(ErrorCode errorCode, String message, Map<String, Object> details, Throwable cause) {
        this(errorCode, message, null, details, cause);
    }

    public DataValidationException(
            ErrorCode errorCode,
            String message,
            DataExecutionResponse response,
            Map<String, Object> details,
            Throwable cause) {
        super(message, cause);
        this.details = details;
        this.errorCode = errorCode;
        this.response = response;
    }
}
