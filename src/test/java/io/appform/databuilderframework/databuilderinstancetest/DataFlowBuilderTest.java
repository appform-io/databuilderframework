package io.appform.databuilderframework.databuilderinstancetest;

import io.appform.databuilderframework.annotations.DataBuilderClassInfo;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.model.Data;
import io.appform.databuilderframework.model.DataAdapter;
import io.appform.databuilderframework.model.DataDelta;
import io.appform.databuilderframework.model.DataFlowInstance;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DataFlowBuilderTest {

    private static final class TestDataA extends DataAdapter<TestDataA> {
        private final String value;
        public TestDataA(final String value) {
            super(TestDataA.class);
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    private static final class TestDataB extends DataAdapter<TestDataB> {
        private final String value;
        public TestDataB(final String value) {
            super(TestDataB.class);
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    private static final class TestDataC extends DataAdapter<TestDataC> {
        private final String value;
        public TestDataC(final String value) {
            super(TestDataC.class);
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    @DataBuilderClassInfo(produces = TestDataC.class, consumes = {TestDataA.class, TestDataB.class})
    private static final class BuilderA extends DataBuilder {
        @Override
        public Data process(DataBuilderContext context) throws DataBuilderException {
            DataSetAccessor dataSetAccessor = context.getDataSet().accessor();
            return new TestDataC(dataSetAccessor.get(TestDataA.class).getValue()
                                + " "
                                + dataSetAccessor.get(TestDataB.class).getValue());
        }
    }

    @Test
    public void testInstantiatingBuilder() throws Exception {
        val dataFlow = new DataFlowBuilder()
                                        .withDataBuilder(new BuilderA())
                                        .withTargetData(TestDataC.class)
                                        .build();
        val executor = new SimpleDataFlowExecutor();
        val instance = new DataFlowInstance();
        instance.setDataFlow(dataFlow);
        val response = executor.run(instance,
                                                      new DataDelta(
                                                          new TestDataA("Hello"),
                                                          new TestDataB("Santanu")));
        assertTrue(response.getResponses().containsKey(Utils.name(TestDataC.class)));
    }

}