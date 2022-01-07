package io.appform.databuilderframework.engine;

import io.appform.databuilderframework.model.Data;
import io.appform.databuilderframework.model.DataBuilderMeta;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class ProxyDataBuilder extends DataBuilder {
    private final DataBuilder impl;

    ProxyDataBuilder(DataBuilderMeta dataBuilderMeta, DataBuilder impl) {
        this.impl = impl;
        setDataBuilderMeta(dataBuilderMeta);
        impl.setDataBuilderMeta(dataBuilderMeta);
    }

    @Override
    public Data process(DataBuilderContext context) throws DataBuilderException, DataValidationException {
        return impl.process(context);
    }
}
