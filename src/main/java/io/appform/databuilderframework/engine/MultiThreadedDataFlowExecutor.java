package io.appform.databuilderframework.engine;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * The executor for a {@link io.appform.databuilderframework.model.DataFlow}.
 */
@Slf4j
@SuppressWarnings("java:S1181")
public class MultiThreadedDataFlowExecutor extends DataFlowExecutor {

    private final ExecutorService executorService;

    public MultiThreadedDataFlowExecutor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public MultiThreadedDataFlowExecutor(DataBuilderFactory dataBuilderFactory, ExecutorService executorService) {
        super(dataBuilderFactory);
        this.executorService = executorService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataExecutionResponse run(
            DataBuilderContext dataBuilderContext,
            DataFlowInstance dataFlowInstance,
            DataDelta dataDelta,
            DataFlow dataFlow,
            DataBuilderFactory builderFactory) throws DataBuilderFrameworkException, DataValidationException {
        val completionExecutor = new ExecutorCompletionService<DataContainer>(executorService);
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
                List<Future<DataContainer>> dataFutures = startJobs(dataBuilderContext,
                                                                    dataFlowInstance,
                                                                    dataDelta,
                                                                    builderFactory,
                                                                    completionExecutor,
                                                                    dataSet,
                                                                    dataSetAccessor,
                                                                    responseData,
                                                                    activeDataSet,
                                                                    processedBuilders,
                                                                    levelBuilders);


                //Now wait for something to complete.
                val listSize = dataFutures.size();
                for (int i = 0; i < listSize; i++) {
                    try {
                        val responseContainer = completionExecutor.take().get();
                        val response = responseContainer.getGeneratedData();
                        val data = responseContainer.getBuilderMeta().getProduces();
                        if (responseContainer.isHasError()) {
                            if (null != responseContainer.getValidationException()) {
                                throw responseContainer.getValidationException();
                            }

                            throw responseContainer.getException();
                        }
                        if (null != response) {
                            Preconditions.checkArgument(response.getData().equalsIgnoreCase(data),
                                                                "Builder is supposed to produce %s but produces %s",
                                                                data,
                                                                response.getData());
                            dataSetAccessor.merge(response);
                            responseData.put(response.getData(), response);
                            activeDataSet.add(response.getData());
                            if (null != dataFlow.getTransients() && !dataFlow.getTransients()
                                    .contains(response.getData())) {
                                newlyGeneratedData.add(response.getData());
                            }
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.BUILDER_EXECUTION_ERROR,
                                                                "Error while waiting for error ", e);
                    }
                    catch (ExecutionException e) {
                        throw new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.BUILDER_EXECUTION_ERROR,
                                                                "Error while waiting for error ", e.getCause());
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

    @SuppressWarnings("java:S107")
    private List<Future<DataContainer>> startJobs(
            DataBuilderContext dataBuilderContext,
            DataFlowInstance dataFlowInstance,
            DataDelta dataDelta,
            DataBuilderFactory builderFactory,
            ExecutorCompletionService<DataContainer> completionExecutor,
            DataSet dataSet,
            DataSetAccessor dataSetAccessor,
            TreeMap<String, Data> responseData,
            HashSet<String> activeDataSet,
            Set<DataBuilderMeta> processedBuilders,
            List<DataBuilderMeta> levelBuilders) {
        return levelBuilders.stream()
                .filter(builderMeta -> !processedBuilders.contains(builderMeta)
                        //If there is an intersection, means some of it's inputs have changed. Reevaluate
                        && !Sets.intersection(builderMeta.getEffectiveConsumes(), activeDataSet).isEmpty())
                .map(builderMeta -> {
                    val builder = builderFactory.create(builderMeta);
                    if (!dataSetAccessor.checkForData(builder.getDataBuilderMeta().getConsumes())) {
                        return null;
                    }
                    return completionExecutor.submit(
                            new BuilderRunner(dataBuilderExecutionListener,
                                              dataFlowInstance,
                                              dataDelta,
                                              responseData,
                                              builder,
                                              builderMeta,
                                              dataBuilderContext,
                                              processedBuilders,
                                              dataSet));
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }


}
