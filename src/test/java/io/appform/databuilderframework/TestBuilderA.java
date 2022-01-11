package io.appform.databuilderframework;

import io.appform.databuilderframework.annotations.DataBuilderInfo;
import io.appform.databuilderframework.engine.DataBuilder;
import io.appform.databuilderframework.engine.DataBuilderContext;
import io.appform.databuilderframework.model.Data;
import lombok.val;

@DataBuilderInfo(name = "BuilderA", consumes = {"B", "A"}, produces = "C")
public class TestBuilderA extends DataBuilder {

    @Override
    public Data process(DataBuilderContext context) {
        val dataSetAccessor = context.getDataSet().accessor();
        val a = dataSetAccessor.get("A", TestDataA.class);
        val b = dataSetAccessor.get("B", TestDataB.class);
        return new TestDataC(a.getValue() + " " + b.getValue());
    }
}
