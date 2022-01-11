package io.appform.databuilderframework.engine;

import io.appform.databuilderframework.model.DataBuilderMeta;

/**
 * A generic factory to build any {@link DataBuilder}.
 * Used by {@link DataFlowExecutor} during flow execution.
 */
public interface DataBuilderFactory {
    /**
     * Create a {@link DataBuilder} of the given name.
     * @param dataBuilderMeta Meta for the builder to create
     * @return A {@link DataBuilder}
     * @throws DataBuilderFrameworkException Throws error if creation failed
     */
    DataBuilder create(DataBuilderMeta dataBuilderMeta) throws DataBuilderFrameworkException;

    /**
     * Create and return an immutable copy of the factory to be used during execution.
     * NOTE: If you are unable to return an immutable copy, at least ensure that the returned version is thread safe,
     * specially if you plan to use the {@link io.appform.databuilderframework.engine.MultiThreadedDataFlowExecutor}
     * @return An immutable copy of the factory.
     */
    DataBuilderFactory immutableCopy();
}
