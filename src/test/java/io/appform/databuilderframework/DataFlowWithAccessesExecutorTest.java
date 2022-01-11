package io.appform.databuilderframework;

import com.google.common.collect.Lists;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.*;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.*;

public class DataFlowWithAccessesExecutorTest {

    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager();
    private final DataFlowExecutor executor = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(
            dataBuilderMetadataManager));
    private final DataFlow dataFlow = new DataFlowBuilder()
            .withAnnotatedDataBuilder(TestBuilderAccesses.class)
            .withTargetData("X")
            .build();


    @Test
    public void withoutAnyAccessData() throws DataBuilderFrameworkException, DataValidationException {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataA("Hello")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertFalse(response.getResponses().isEmpty());
            assertTrue(response.getResponses().containsKey("X"));
            if (response.getResponses().get("X") instanceof TestDataX) {
                assertEquals("FALSE", ((TestDataX) response.getResponses().get("X")).getValue());
            }
            else {
                fail("X not instance of TestDataX");
            }
        }
    }

    @Test
    public void withAccessData() throws DataBuilderFrameworkException, DataValidationException {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataA("Hello"), new TestDataD("DD")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertFalse(response.getResponses().isEmpty());
            assertTrue(response.getResponses().containsKey("X"));
            if (response.getResponses().get("X") instanceof TestDataX) {
                assertEquals("TRUE", ((TestDataX) response.getResponses().get("X")).getValue());
            }
            else {
                fail("X not instance of TestDataX");
            }
        }
    }

    @Test
    public void withoutOnlyAccessData() throws DataBuilderFrameworkException, DataValidationException {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            DataDelta dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataD("Hello")));
            DataExecutionResponse response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().isEmpty());
        }
    }


}