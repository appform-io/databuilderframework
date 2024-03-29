package io.appform.databuilderframework.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

/**
 * A instance of the {@link io.appform.databuilderframework.model.DataFlow} object to be used for execution.
 * This class represents a running instance of the flow. It contains the flow itself as well as the
 * {@link io.appform.databuilderframework.model.DataSet} required for execution.
 */
@Data
public class DataFlowInstance {

    /**
     * The ID of the flow instance.
     */
    @NotNull
    @NotEmpty
    @JsonProperty
    private String id;

    /**
     * The {@link io.appform.databuilderframework.model.DataFlow} on which the instance is based.
     */
    @JsonProperty
    private DataFlow dataFlow;

    /**
     * The working {@link io.appform.databuilderframework.model.DataSet} for the flow.
     */
    @JsonProperty
    private DataSet dataSet;

    public DataFlowInstance() {
        this.dataSet = new DataSet();
    }

    public DataFlowInstance(String id, DataFlow dataFlow, DataSet dataSet) {
        this.id = id;
        this.dataFlow = dataFlow;
        this.dataSet = dataSet;
    }

    public DataFlowInstance(String id, DataFlow dataFlow) {
        this(id, dataFlow, new DataSet());
    }
}
