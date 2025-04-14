package io.appform.databuilderframework.engine;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * The executor for a {@link io.appform.databuilderframework.model.DataFlow}.
 */
@Slf4j
@SuppressWarnings("java:S1181")
public class SimpleDataFlowExecutor extends DataFlowExecutor {

    public SimpleDataFlowExecutor() {
        this(null);
    }

    public SimpleDataFlowExecutor(DataBuilderFactory dataBuilderFactory) {
        super(dataBuilderFactory);
    }

    /*
     * {@inheritDoc}
     */
    protected DataExecutionResponse run(
            DataBuilderContext dataBuilderContext,
            DataFlowInstance dataFlowInstance,
            DataDelta dataDelta,
            DataFlow dataFlow,
            DataBuilderFactory builderFactory) throws DataBuilderFrameworkException, DataValidationException {
        val executionGraph = dataFlow.getExecutionGraph();
        val dataSet = dataFlowInstance.getDataSet().accessor().copy(); //Create own copy to work with
        val dataSetAccessor = DataSet.accessor(dataSet);
        val responseData = new TreeMap<String, Data>();
        val activeDataSet = new HashSet<String>();
        val dependencyHierarchy = executionGraph.getDependencyHierarchy();
        val newlyGeneratedData = new HashSet<String>();
        val processedBuilders = Collections.synchronizedSet(new HashSet<DataBuilderMeta>());

        dataSetAccessor.merge(dataDelta);
        dataDelta.getDelta().forEach(data -> activeDataSet.add(data.getData()));

        boolean completed = false;
        while (!completed) {
            for (val levelBuilders : dependencyHierarchy) {
                for (val builderMeta : levelBuilders) {
                    if (processedBuilders.contains(builderMeta)) {
                        continue;
                    }
                    //If there is an intersection, means some of it's inputs have changed. Reevaluate
                    if (Sets.intersection(builderMeta.getEffectiveConsumes(), activeDataSet).isEmpty()) {
                        continue;
                    }
                    DataBuilder builder = builderFactory.create(builderMeta);
                    if (!dataSetAccessor.checkForData(builder.getDataBuilderMeta().getConsumes())) {
                        continue;
                    }
                    for (DataBuilderExecutionListener listener : dataBuilderExecutionListener) {
                        try {
                            listener.beforeExecute(dataBuilderContext,
                                                   dataFlowInstance,
                                                   builderMeta,
                                                   dataDelta,
                                                   responseData);
                        }
                        catch (Throwable t) {
                            log.error("Error running pre-execution execution listener: ", t);
                        }
                    }
                    try {
                        Data response = builder.process(
                                dataBuilderContext.immutableCopy(
                                        dataSet.accessor().getAccesibleDataSetFor(builder)));
                        if (null != response) {
                            Preconditions.checkArgument(response.getData().equalsIgnoreCase(builderMeta.getProduces()),
                                            "Builder is supposed to produce %s but produces %s",
                                            builderMeta.getProduces(),
                                            response.getData());
                            dataSetAccessor.merge(response);
                            responseData.put(response.getData(), response);
                            response.setGeneratedBy(builderMeta.getName());
                            activeDataSet.add(response.getData());
                            if (null != dataFlow.getTransients() && !dataFlow.getTransients()
                                    .contains(response.getData())) {
                                newlyGeneratedData.add(response.getData());
                            }
                        }
                        log.trace("Ran " + builderMeta.getName());
                        processedBuilders.add(builderMeta);
                        for (DataBuilderExecutionListener listener : dataBuilderExecutionListener) {
                            try {
                                listener.afterExecute(dataBuilderContext,
                                                      dataFlowInstance,
                                                      builderMeta,
                                                      dataDelta,
                                                      responseData,
                                                      response);
                            }
                            catch (Throwable t) {
                                log.error("Error running post-execution listener: ", t);
                            }
                        }

                    }
                    catch (DataBuilderException e) {
                        executePostExceptionListeners(dataBuilderContext,
                                                      dataFlowInstance,
                                                      dataDelta,
                                                      responseData,
                                                      builderMeta,
                                                      e);

                        throw new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.BUILDER_EXECUTION_ERROR,
                                                                "Error running builder: " + builderMeta.getName(),
                                                                e.getDetails(),
                                                                e,
                                                                new DataExecutionResponse(responseData));

                    }
                    catch (DataValidationException e) {
                        executePostExceptionListeners(dataBuilderContext,
                                                      dataFlowInstance,
                                                      dataDelta,
                                                      responseData,
                                                      builderMeta,
                                                      e);
                        // Sending Execution response in exception object

                        throw new DataValidationException(DataValidationException.ErrorCode.DATA_VALIDATION_EXCEPTION,
                                                          e.getMessage(),
                                                          new DataExecutionResponse(responseData),
                                                          e.getDetails(),
                                                          e);


                    }
                    catch (Throwable t) {
                        executePostExceptionListeners(dataBuilderContext,
                                                      dataFlowInstance,
                                                      dataDelta,
                                                      responseData,
                                                      builderMeta,
                                                      t);
                        throw new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.BUILDER_EXECUTION_ERROR,
                                                                "Error running builder: " + builderMeta.getName()
                                                                        + ": " + t.getMessage(),
                                                                Collections.singletonMap("MESSAGE", t.getMessage()),
                                                                t,
                                                                new DataExecutionResponse(responseData));
                    }
                }
            }
            completed = newlyGeneratedData.contains(dataFlow.getTargetData())
                    || newlyGeneratedData.isEmpty();
            if (newlyGeneratedData.contains(dataFlow.getTargetData())) {
                log.trace("Finished running this instance of the flow. Exiting.");
            }
            if (newlyGeneratedData.isEmpty()) {
                log.trace("Nothing happened in this loop, exiting..");
            }
            //Some data generated which is not the target. But we're done here
            log.trace("Newly generated: {}", newlyGeneratedData);
            activeDataSet.clear();
            activeDataSet.addAll(newlyGeneratedData);
            newlyGeneratedData.clear();
            completed = completed || !dataFlow.isLoopingEnabled();
        }
        DataSet finalDataSet = dataSetAccessor.copy(dataFlow.getTransients());
        dataFlowInstance.setDataSet(finalDataSet);
        return new DataExecutionResponse(responseData);
    }

    private void executePostExceptionListeners(
            DataBuilderContext dataBuilderContext,
            DataFlowInstance dataFlowInstance,
            DataDelta dataDelta,
            Map<String, Data> responseData,
            DataBuilderMeta builderMeta,
            Throwable e) {
        log.error("Error running builder: {}", builderMeta.getName());
        for (DataBuilderExecutionListener listener : dataBuilderExecutionListener) {
            try {
                listener.afterException(dataBuilderContext, dataFlowInstance, builderMeta, dataDelta, responseData, e);

            }
            catch (Throwable error) {
                log.error("Error running post-execution listener: ", error);
            }
        }
    }

}
