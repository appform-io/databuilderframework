package io.appform.databuilderframework.engine;


import io.appform.databuilderframework.model.DataExecutionResponse;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

import java.util.Collections;
import java.util.Map;

@Value
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class DataBuilderFrameworkException extends RuntimeException {

    public enum ErrorCode {
        PRE_PROCESSING_ERROR,
        POST_PROCESSING_ERROR,
        NO_FACTORY_FOR_DATA_BUILDER,
        NO_BUILDER_FOR_DATA,
        BUILDER_EXISTS,
        NO_TARGET_DATA,
        NO_BUILDER_FOUND_FOR_NAME,
        INSTANTIATION_FAILURE,
        BUILDER_RESOLUTION_CONFLICT_FOR_DATA,
        BUILDER_EXECUTION_ERROR
    }

    ErrorCode errorCode;
    Map<String, Object> details;
    DataExecutionResponse partialExecutionResponse;

    public DataBuilderFrameworkException(ErrorCode errorCode, String message) {
        this(errorCode, message, Collections.emptyMap());
    }

    public DataBuilderFrameworkException(ErrorCode errorCode, String message, Throwable cause) {
        this(errorCode, message, Collections.emptyMap(), cause);
    }

    public DataBuilderFrameworkException(ErrorCode errorCode, String message, Map<String, Object> details) {
        this(errorCode, message, details, null);
    }

    public DataBuilderFrameworkException(
            ErrorCode errorCode,
            String message,
            Map<String, Object> details,
            Throwable cause) {
        this(errorCode, message, details, cause, null);
    }

    public DataBuilderFrameworkException(
            ErrorCode errorCode,
            String message,
            Map<String, Object> details,
            Throwable cause,
            DataExecutionResponse partialExecutionResponse) {
        super(message, cause);
        this.errorCode = errorCode;
        this.details = details;
        this.partialExecutionResponse = partialExecutionResponse;
    }
}
