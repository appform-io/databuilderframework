package io.appform.databuilderframework.engine.impl;

import io.appform.databuilderframework.engine.DataBuilder;
import io.appform.databuilderframework.engine.DataBuilderFactory;
import io.appform.databuilderframework.engine.DataBuilderFrameworkException;
import io.appform.databuilderframework.engine.DataBuilderMetadataManager;
import io.appform.databuilderframework.model.DataBuilderMeta;
import lombok.val;

/**
 * @inheritDoc This particular version, uses metadata stored in {@link io.appform.databuilderframework.engine.DataBuilderMetadataManager}
 * to generate a specific builder.
 */
public class InstantiatingDataBuilderFactory implements DataBuilderFactory {
    private final DataBuilderMetadataManager dataBuilderMetadataManager;
    private final boolean useCurrentMeta;

    public InstantiatingDataBuilderFactory(DataBuilderMetadataManager dataBuilderMetadataManager) {
        this(dataBuilderMetadataManager, false);
    }

    public InstantiatingDataBuilderFactory(
            DataBuilderMetadataManager dataBuilderMetadataManager,
            boolean useCurrentMeta) {
        this.dataBuilderMetadataManager = dataBuilderMetadataManager;
        this.useCurrentMeta = useCurrentMeta;
    }

    public DataBuilder create(DataBuilderMeta dataBuilderMeta) throws DataBuilderFrameworkException {
        val builderName = dataBuilderMeta.getName();
        val dataBuilderClass = dataBuilderMetadataManager.getDataBuilderClass(builderName);
        if (null == dataBuilderClass) {
            throw new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.NO_BUILDER_FOUND_FOR_NAME,
                                                    "No builder found for name: " + builderName);
        }
        try {
            val dataBuilder = dataBuilderClass.newInstance();
            return useCurrentMeta
                    ? dataBuilder.setDataBuilderMeta(dataBuilderMetadataManager.get(builderName).deepCopy())
                   : dataBuilder.setDataBuilderMeta(dataBuilderMeta.deepCopy());
        }
        catch (Exception e) {
            throw new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.INSTANTIATION_FAILURE,
                                                    "Could not instantiate builder: " + builderName);
        }
    }

    @Override
    public DataBuilderFactory immutableCopy() {
        return new InstantiatingDataBuilderFactory(dataBuilderMetadataManager.immutableCopy());
    }
}
