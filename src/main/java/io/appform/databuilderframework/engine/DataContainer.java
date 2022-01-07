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

import io.appform.databuilderframework.model.Data;
import io.appform.databuilderframework.model.DataBuilderMeta;
import lombok.Value;

/**
 *
 */
@Value
public class DataContainer {
    DataBuilderMeta builderMeta;
    Data generatedData;
    boolean hasError;
    DataBuilderFrameworkException exception;
    DataValidationException validationException;

    DataContainer(DataBuilderMeta builderMeta, Data generatedData) {
        this(builderMeta, generatedData, false, null, null);
    }

    DataContainer(DataBuilderMeta builderMeta, DataBuilderFrameworkException exception) {
        this(builderMeta, null, true, exception, null);

    }

    DataContainer(DataBuilderMeta builderMeta, DataValidationException validationException) {
        this(builderMeta, null, true, null, validationException);
    }


    DataContainer(
            DataBuilderMeta builderMeta,
            Data generatedData,
            boolean hasError,
            DataBuilderFrameworkException exception,
            DataValidationException validationException) {
        this.builderMeta = builderMeta;
        this.generatedData = generatedData;
        this.hasError = hasError;
        this.exception = exception;
        this.validationException = validationException;
    }
}
