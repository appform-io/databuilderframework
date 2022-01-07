package io.appform.databuilderframework;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.*;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConditionalFlowAccessTest {
    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager()
            .register(ImmutableSet.of("A", "B"), "C", "BuilderA", TestBuilderA.class)
            .registerWithAccess(ImmutableSet.of("C", "D"),
                                ImmutableSet.of("A"),
                                "E",
                                "BuilderB",
                                ConditionalBuilder.class)
            .register(ImmutableSet.of("A", "E"), "F", "BuilderC", TestBuilderC.class);

    private final DataFlowExecutor executor
            = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(dataBuilderMetadataManager));

    private final DataFlow dataFlow = new DataFlowBuilder()
            .withMetaDataManager(dataBuilderMetadataManager)
            .withTargetData("F")
            .build();

    public static final class ConditionalBuilder extends DataBuilder {

        @Override
        public Data process(DataBuilderContext context) throws DataBuilderException {
            val accessor = new DataSetAccessor(context.getDataSet());
            assertTrue(accessor.checkForData("A")); //Assuming in this test without BuilderA Builder is not run and Builder B has access to A
            assertFalse(accessor.checkForData("B")); //BuilderB does not have access to A
            val dataC = accessor.get("C", TestDataC.class);
            val dataD = accessor.get("D", TestDataD.class);
            if (dataC.getValue().equals("Hello World")
                    && dataD.getValue().equalsIgnoreCase("this")) {
                return new TestDataE("Wah wah!!");
            }
            return null;
        }
    }

    @Test
    public void testRunStop() throws Exception {
        val dataFlowInstance = new DataFlowInstance();
        dataFlowInstance.setId("testflow");
        dataFlowInstance.setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataA("Hello")));
            DataExecutionResponse response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().isEmpty());
        }
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataB("Bhai")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertFalse(response.getResponses().isEmpty());
            assertTrue(response.getResponses().containsKey("C"));
        }
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataD("this")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().isEmpty());
        }

    }

    @Test
    public void testRun() throws Exception {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataA("Hello")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().isEmpty());
        }
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataB("World")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertFalse(response.getResponses().isEmpty());
            assertTrue(response.getResponses().containsKey("C"));
        }
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataD("this")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertFalse(response.getResponses().isEmpty());
            assertTrue(response.getResponses().containsKey("E"));
            assertTrue(response.getResponses().containsKey("F"));
        }

    }

}
