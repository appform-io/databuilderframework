/*
 * Copyright (c) 2022 $user
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.appform.databuilderframework.engine;

import com.google.common.base.Preconditions;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 *
 */
@Slf4j
@SuppressWarnings("java:S1181")
final class BuilderRunner implements Callable<DataContainer> {
    private static final String BUILDER_EXEC_ERR_MSG = "Error running builder: ";

    private final List<DataBuilderExecutionListener> dataBuilderExecutionListener;
    private final DataFlowInstance dataFlowInstance;
    private final DataDelta dataDelta;
    private final Map<String, Data> responseData;
    private final DataBuilder builder;
    private final DataBuilderMeta builderMeta;
    private final DataBuilderContext dataBuilderContext;
    private final Set<DataBuilderMeta> procesedBuilders;
    private final DataSet dataSet;

    @SuppressWarnings("java:S107")
    BuilderRunner(
            List<DataBuilderExecutionListener> dataBuilderExecutionListener,
            DataFlowInstance dataFlowInstance,
            DataDelta dataDelta,
            Map<String, Data> responseData,
            DataBuilder builder,
            DataBuilderMeta builderMeta, DataBuilderContext dataBuilderContext,
            Set<DataBuilderMeta> procesedBuilders,
            DataSet dataSet) {
        this.dataBuilderExecutionListener = dataBuilderExecutionListener;
        this.dataFlowInstance = dataFlowInstance;
        this.dataDelta = dataDelta;
        this.responseData = responseData;
        this.builder = builder;
        this.builderMeta = builderMeta;
        this.dataBuilderContext = dataBuilderContext;
        this.procesedBuilders = procesedBuilders;
        this.dataSet = dataSet;
    }

    @Override
    public DataContainer call() throws Exception {

        executePreListeners();
        try {
            Data response = builder.process(dataBuilderContext.immutableCopy(
                    dataSet.accessor().getAccesibleDataSetFor(builder)));
            procesedBuilders.add(builderMeta);
            executePostListenersSuccess(response);
            if (null != response) {
                Preconditions.checkArgument(response.getData().equalsIgnoreCase(builderMeta.getProduces()),
                                            String.format("Builder is supposed to produce %s but produces %s",
                                                          builderMeta.getProduces(), response.getData()));
                response.setGeneratedBy(builderMeta.getName());
            }
            return new DataContainer(builderMeta, response);
        }
        catch (DataBuilderException e) {
            executePostExceptionListeners(e);
            return new DataContainer(builderMeta,
                                     new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.BUILDER_EXECUTION_ERROR,
                                                                       BUILDER_EXEC_ERR_MSG + builderMeta.getName(),
                                                                       e.getDetails(),
                                                                       e));

        }
        catch (DataValidationException e) {
            executePostExceptionListeners(e);
            return new DataContainer(builderMeta,
                                     new DataValidationException(DataValidationException.ErrorCode.DATA_VALIDATION_EXCEPTION,
                                                                 BUILDER_EXEC_ERR_MSG + builderMeta.getName(),
                                                                 new DataExecutionResponse(responseData),
                                                                 e.getDetails(),
                                                                 e));


        }
        catch (Throwable t) {
            executePostExceptionListeners(t);
            return new DataContainer(builderMeta,
                                     new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.BUILDER_EXECUTION_ERROR,
                                                                       BUILDER_EXEC_ERR_MSG + builderMeta.getName() + t.getMessage(),
                                                                       Collections.singletonMap("MESSAGE",
                                                                                                t.getMessage()),
                                                                       t));
        }
    }

    private void executePostListenersSuccess(Data response) {
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

    private void executePostExceptionListeners(Throwable e) {
        log.error("Error running builder: {}", builderMeta.getName());
        for (DataBuilderExecutionListener listener : dataBuilderExecutionListener) {
            try {
                listener.afterException(dataBuilderContext,
                                        dataFlowInstance,
                                        builderMeta,
                                        dataDelta,
                                        responseData,
                                        e);

            }
            catch (Throwable error) {
                log.error("Error running post-execution listener: ", error);
            }
        }
    }

    private void executePreListeners() {
        for (DataBuilderExecutionListener listener : dataBuilderExecutionListener) {
            try {
                listener.beforeExecute(dataBuilderContext, dataFlowInstance, builderMeta, dataDelta, responseData);
            }
            catch (Throwable t) {
                log.error("Error running pre-execution execution listener: ", t);
            }
        }
    }
}
