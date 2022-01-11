package io.appform.databuilderframework.engine;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.*;
import java.util.concurrent.*;

/**
 * The executor for a {@link io.appform.databuilderframework.model.DataFlow}.
 */
@Slf4j
public class OptimizedMultiThreadedDataFlowExecutor extends DataFlowExecutor {
    private final ExecutorService executorService;

    public OptimizedMultiThreadedDataFlowExecutor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public OptimizedMultiThreadedDataFlowExecutor(
            DataBuilderFactory dataBuilderFactory,
            ExecutorService executorService) {
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
        dataSetAccessor.merge(dataDelta);
        val responseData = Maps.<String, Data>newTreeMap();
        val activeDataSet = Sets.<String>newHashSet();

        dataDelta.getDelta().stream().map(Data::getData).forEach(activeDataSet::add);

        List<List<DataBuilderMeta>> dependencyHierarchy = executionGraph.getDependencyHierarchy();
        Set<String> newlyGeneratedData = Sets.newHashSet();
        Set<DataBuilderMeta> processedBuilders = Collections.synchronizedSet(Sets.<DataBuilderMeta>newHashSet());
        while (true) {
            for (List<DataBuilderMeta> levelBuilders : dependencyHierarchy) {
                val dataFutures = Lists.newArrayList();
                BuilderRunner singleRef = null; //refrence to builderRunner when size of levelBuilders == 1 to avoid running it behind thread
                for (DataBuilderMeta builderMeta : levelBuilders) {
                    if (processedBuilders.contains(builderMeta)) {
                        continue;
                    }
                    //If there is an intersection, means some of it's inputs have changed. Reevaluate
                    if (Sets.intersection(builderMeta.getEffectiveConsumes(), activeDataSet).isEmpty()) {
                        continue;
                    }
                    val builder = builderFactory.create(builderMeta);
                    if (!dataSetAccessor.checkForData(builder.getDataBuilderMeta().getConsumes())) {
                        continue;
                    }
                    val builderRunner = new BuilderRunner(dataBuilderExecutionListener, dataFlowInstance,
                                                          dataDelta, responseData,
                                                          builder,
                                                          builderMeta,
                                                          dataBuilderContext, processedBuilders, dataSet);

                    if (levelBuilders.size() == 1) {
                        singleRef = builderRunner;
                    }
                    else {
                        dataFutures.add(completionExecutor.submit(builderRunner));
                    }
                }

                //Now wait for something to complete.
                val listSize = dataFutures.size();
                for (int i = 0; i < listSize || singleRef != null; i++) { //listSize == 0 when singleRef is present, or condition allows this logic to run once
                    try {
                        DataContainer responseContainer = null;
                        if (singleRef != null) {
                            try {
                                responseContainer = singleRef.call();
                            }
                            catch (Exception e) {
                                throw new ExecutionException(e); //to map this to existing exception handling
                            }
                            finally {
                                singleRef = null; // make this null to avoid loopback hell
                            }
                        }
                        else {
                            responseContainer = completionExecutor.take().get();
                        }

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
                                                        String.format(
                                                                "Builder is supposed to produce %s but produces %s",
                                                                data,
                                                                response.getData()));
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
            if (newlyGeneratedData.contains(dataFlow.getTargetData())) {
                log.trace("Finished running this instance of the flow. Exiting.");
                break;
            }
            if (newlyGeneratedData.isEmpty()) {
                log.trace("Nothing happened in this loop, exiting..");
                break;
            }
//            StringBuilder stringBuilder = new StringBuilder();
//            for(String data : newlyGeneratedData) {
//                stringBuilder.append(data + ", ");
//            }
            log.trace("Newly generated: {}", newlyGeneratedData);
            activeDataSet.clear();
            activeDataSet.addAll(newlyGeneratedData);
            newlyGeneratedData.clear();
            if (!dataFlow.isLoopingEnabled()) {
                break;
            }
        }
        dataFlowInstance.setDataSet(dataSetAccessor.copy(dataFlow.getTransients()));
        return new DataExecutionResponse(responseData);
    }

}
