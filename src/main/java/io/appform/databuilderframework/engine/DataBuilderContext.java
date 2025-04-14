package io.appform.databuilderframework.engine;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.appform.databuilderframework.model.DataSet;
import lombok.Builder;
import lombok.val;

import java.util.Collections;
import java.util.Map;

/**
 * Context object passed to the builder
 */
public class DataBuilderContext {

    /**
     * The working {@link io.appform.databuilderframework.model.DataSet} for this system.
     */
    private DataSet dataSet;

    private Map<String, Object> contextData;

    public DataBuilderContext() {
        contextData = Maps.newHashMap();
    }

    @Builder
    DataBuilderContext(DataSet dataSet, Map<String, Object> contextData) {
        this.dataSet = dataSet;
        this.contextData = contextData;
    }

    /**
     * Access the data set. Ideally a builder should only access data as declared.
     *
     * @return The full data set.
     */
    public DataSet getDataSet() {
        return dataSet;
    }

    /**
     * Some data to be saved in the context. This data is read-only from inside the builder.
     *
     * @param key   Key for the data.
     * @param value The data value
     * @param <T>   Type of the data
     */
    public <T> DataBuilderContext saveContextData(String key, T value) {
        if (null == key || key.isEmpty()) {
            throw new UncheckedExecutionException(
                    new IllegalArgumentException("Invalid key for context data. Key cannot be null/empty"));
        }
        contextData.put(key, value);
        return this;
    }

    /**
     * Get the data saved with the key
     *
     * @param key    Key to retrieve
     * @param tClass Type of data
     * @param <T>    Type of data
     * @return Value if found, null otherwise
     */
    public <T> T getContextData(String key, Class<T> tClass) {
        Object value = contextData.get(key);
        if (null == value) {
            return null;
        }
        return tClass.cast(value);
    }

    public DataBuilderContext immutableCopy(DataSet dataSet) {
        return new DataBuilderContext(dataSet, ImmutableMap.copyOf(contextData));
    }
}
