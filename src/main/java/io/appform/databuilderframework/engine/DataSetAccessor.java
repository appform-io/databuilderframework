package io.appform.databuilderframework.engine;

import com.google.common.base.Preconditions;
import io.appform.databuilderframework.model.Data;
import io.appform.databuilderframework.model.DataDelta;
import io.appform.databuilderframework.model.DataSet;
import io.appform.databuilderframework.model.DataSetView;
import lombok.val;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

/**
 * Accessor for data access from DataSet.
 * Abstracts out access patterns from the {@link io.appform.databuilderframework.model.DataSet}
 */
public class DataSetAccessor {

    private final DataSet dataSet;

    public DataSetAccessor(DataSet dataSet) {
        this.dataSet = dataSet;
    }

    public <T extends Data> T get(Class<T> tClass) {
        return get(Utils.name(tClass), tClass);
    }

    /**
     * Get a data from {@link io.appform.databuilderframework.model.DataSet}.
     *
     * @param key    Key for the data
     * @param tClass Class to cast the data to. Should inherit from {@link io.appform.databuilderframework.model.Data}
     * @param <T>    Sub-Type for {@link io.appform.databuilderframework.model.Data}. Should be same as <i>tClass</i>
     * @return data
     */
    public <T extends Data> T get(String key, Class<T> tClass) {
        val data = dataSet.get(key);
        return null == data
                ? null
                : tClass.cast(data);
    }

    /**
     * Get data from {@link io.appform.databuilderframework.model.DataSet}, with accessibility checks.
     * This will throw an exception if the calling data builder is not supposed to use the requested data.
     *
     * @param key     Key for the data
     * @param builder {@link io.appform.databuilderframework.engine.DataBuilder} that is accessing the data
     * @param tClass  Class to cast the data to. Should inherit from {@link io.appform.databuilderframework.model.Data}
     * @param <T>     Sub-Type for {@link io.appform.databuilderframework.model.Data}. Should be same as <i>tClass</i>
     * @return data
     */
    public <B extends DataBuilder, T extends Data> T getAccessibleData(String key, B builder, Class<T> tClass) {
        Preconditions.checkArgument(!builder.getDataBuilderMeta().getAccessibleDataSet().contains(key),
                String.format("Builder %s can access only %s",
                        builder.getDataBuilderMeta().getName(),
                        builder.getDataBuilderMeta().getConsumes()));
        return get(key, tClass);
    }

    /**
     * Get only the accessible data for this builder.
     *
     * @param builder
     * @return
     */
    public DataSet getAccesibleDataSetFor(DataBuilder builder) {
        return new DataSetView(dataSet, builder.getDataBuilderMeta().getAccessibleDataSet());
    }

    /**
     * Merge data with current {@link io.appform.databuilderframework.model.DataSet}.
     * Data will be added to the current data-set and override data with same type
     *
     * @param data {@link io.appform.databuilderframework.model.Data} to be merged.
     */
    public void merge(Data data) {
        dataSet.add(data.getData(), data);
    }

    /**
     * Merge all {@link io.appform.databuilderframework.model.Data} elements from the {@link io.appform.databuilderframework.model.DataDelta}
     * Will overwrite all data present in the current {@link io.appform.databuilderframework.model.DataSet}.
     * Each element of data is identified using Data::getData()
     *
     * @param dataDelta {@link io.appform.databuilderframework.model.DataDelta} to be merged
     */
    public void merge(DataDelta dataDelta) {
        dataSet.add(dataDelta.getDelta());
    }

    /**
     * Check if all specified data is present in the {@link io.appform.databuilderframework.model.DataSet}
     *
     * @param dataList List of all name of all data items to be checked.
     * @return <i>true</i> if all elements are present. <i>false</i> otherwise.
     */
    public boolean checkForData(Set<String> dataList) {
        return dataSet.containsAll(dataList);
    }

    /**
     * Check if a specified data is present in the {@link io.appform.databuilderframework.model.DataSet}
     *
     * @param data Name of the data items to be checked.
     * @return <i>true</i> if all elements are present. <i>false</i> otherwise.
     */
    public boolean checkForData(String data) {
        return dataSet.containsAll(Collections.singleton(data));
    }

    /**
     * Check if a specified data is present in the {@link io.appform.databuilderframework.model.DataSet}
     *
     * @param clazz Class to be checked.
     * @return <i>true</i> if all elements are present. <i>false</i> otherwise.
     */
    public boolean checkForData(Class<?> clazz) {
        return checkForData(Utils.name(clazz));
    }

    /**
     * Get a copy of the underlying data set.
     */
    public DataSet copy() {
        return copy(null);
    }

    /**
     * Get a copy of the underlying data set. Don't copy transients.
     */
    public DataSet copy(Set<String> transients) {
        val dataMap = new HashMap<String, Data>();
        dataSet.copyInto(dataMap, transients);
        return new DataSet(dataMap);
    }
}
