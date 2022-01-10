package io.appform.databuilderframework;

import com.google.common.collect.Lists;
import io.appform.databuilderframework.engine.DataBuilderMetadataManager;
import io.appform.databuilderframework.engine.DataFlowBuilder;
import io.appform.databuilderframework.engine.DataFlowExecutor;
import io.appform.databuilderframework.engine.SimpleDataFlowExecutor;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
public class DataFlowWithOptionalExecutorTest {

    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager();
    private final DataFlowExecutor executor = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(dataBuilderMetadataManager));
    private final DataFlow dataFlow  = new DataFlowBuilder()
            .withAnnotatedDataBuilder(TestBuilderOptional.class)
            .withAnnotatedDataBuilder(TestBuilderB.class)
            .withAnnotatedDataBuilder(TestBuilderC.class)
            .withTargetData("F")
            .build();

    @Test
    public void testRunWithoutOptional() throws Exception {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(new TestDataB("World"));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().isEmpty());
            assertFalse(response.getResponses().containsKey("C"));
        }
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataA("Hello World")));
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

    @Test
    public void testRunOptional() throws Exception {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        
        {
            val dataDelta = new DataDelta(new TestDataA("Hello"));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().isEmpty());
            assertFalse(response.getResponses().containsKey("C"));
        }
        {
            val dataDelta = new DataDelta(new TestDataB("World"));
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
    @Test
    public void testOptionalComingFirst() throws Exception {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(new TestDataB("World"));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().isEmpty());
            assertFalse(response.getResponses().containsKey("C"));
        }
        {
            val dataDelta = new DataDelta(new TestDataA("Hello"));
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
