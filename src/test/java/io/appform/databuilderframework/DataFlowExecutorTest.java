package io.appform.databuilderframework;

import com.google.common.collect.Lists;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

@Slf4j
public class DataFlowExecutorTest {
    private static class TestListener implements DataBuilderExecutionListener {

        @Override
        public void preProcessing(
                DataFlowInstance dataFlowInstance,
                DataDelta dataDelta) {
            log.info("Being called for: {}", dataFlowInstance.getId());
        }

        @Override
        public void beforeExecute(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta, Map<String, Data> prevResponses) throws Exception {
            log.info("{} called for: {}", builderToBeApplied.getName(), dataFlowInstance.getId());
        }

        @Override
        public void afterExecute(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta, Map<String, Data> prevResponses, Data currentResponse) throws Exception {
            log.info("{} called for: {}", builderToBeApplied.getName(), dataFlowInstance.getId());
        }

        @Override
        public void afterException(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta,
                Map<String, Data> prevResponses, Throwable frameworkException) throws Exception {
            log.info("{} called for: {}", builderToBeApplied.getName(), dataFlowInstance.getId());
        }

        @Override
        public void postProcessing(
                DataFlowInstance dataFlowInstance,
                DataDelta dataDelta,
                DataExecutionResponse response,
                Throwable frameworkException) throws Exception {
            log.info("Being called for: {}", dataFlowInstance.getId());
        }
    }

    private static class TestListenerBeforeExecutionError implements DataBuilderExecutionListener {

        @Override
        public void preProcessing(
                DataFlowInstance dataFlowInstance,
                DataDelta dataDelta) throws Exception {
            log.info("Being called for: {}", dataFlowInstance.getId());
        }

        @Override
        public void beforeExecute(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta, Map<String, Data> prevResponses) throws Exception {
            //System.out.println(builderToBeApplied.getName() + " being called for: " + dataFlowInstance.getId());
            throw new Exception("Blah blah");
        }

        @Override
        public void afterExecute(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta, Map<String, Data> prevResponses, Data currentResponse) throws Exception {
            log.info("{} called for: {}", builderToBeApplied.getName(), dataFlowInstance.getId());
        }

        @Override
        public void afterException(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta,
                Map<String, Data> prevResponses, Throwable frameworkException) throws Exception {
            log.info("{} called for: {}", builderToBeApplied.getName(), dataFlowInstance.getId());
        }

        @Override
        public void postProcessing(
                DataFlowInstance dataFlowInstance,
                DataDelta dataDelta,
                DataExecutionResponse response,
                Throwable frameworkException) throws Exception {
            log.info("Being called for: {}", dataFlowInstance.getId());
        }
    }

    private static class TestListenerAfterExecutionError implements DataBuilderExecutionListener {

        @Override
        public void preProcessing(
                DataFlowInstance dataFlowInstance,
                DataDelta dataDelta) throws Exception {
            log.info("Being called for: {}", dataFlowInstance.getId());
        }

        @Override
        public void beforeExecute(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta, Map<String, Data> prevResponses) throws Exception {
            log.info("{} called for: {}", builderToBeApplied.getName(), dataFlowInstance.getId());
        }

        @Override
        public void afterExecute(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta, Map<String, Data> prevResponses, Data currentResponse) throws Exception {
            //System.out.println(builderToBeApplied.getName() + " called for: " + dataFlowInstance.getId());
            throw new Exception("Blah blah");

        }

        @Override
        public void afterException(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta,
                Map<String, Data> prevResponses, Throwable frameworkException) throws Exception {
            log.info("{} called for: {}", builderToBeApplied.getName(), dataFlowInstance.getId());
        }

        @Override
        public void postProcessing(
                DataFlowInstance dataFlowInstance,
                DataDelta dataDelta,
                DataExecutionResponse response,
                Throwable frameworkException) throws Exception {
            log.info("Being called for: {}", dataFlowInstance.getId());
        }
    }

    private static class TestListenerAfterExceptionError implements DataBuilderExecutionListener {

        @Override
        public void preProcessing(
                DataFlowInstance dataFlowInstance,
                DataDelta dataDelta) throws Exception {
            log.info("Being called for: {}", dataFlowInstance.getId());
        }

        @Override
        public void beforeExecute(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta, Map<String, Data> prevResponses) throws Exception {
            log.info("{} called for: {}", builderToBeApplied.getName(), dataFlowInstance.getId());
        }

        @Override
        public void afterExecute(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta, Map<String, Data> prevResponses, Data currentResponse) throws Exception {
            log.info("{} called for: {}", builderToBeApplied.getName(), dataFlowInstance.getId());

        }

        @Override
        public void afterException(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta,
                Map<String, Data> prevResponses, Throwable frameworkException) throws Exception {
            //System.out.println(builderToBeApplied.getName() + " called for: " + dataFlowInstance.getId());
            throw new Exception("Blah blah");
        }

        @Override
        public void postProcessing(
                DataFlowInstance dataFlowInstance,
                DataDelta dataDelta,
                DataExecutionResponse response,
                Throwable frameworkException) throws Exception {
            log.info("Being called for: {}", dataFlowInstance.getId());
        }
    }

    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager();
    private final DataFlowExecutor executor = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(
            dataBuilderMetadataManager))
            .registerExecutionListener(new TestListener())
            .registerExecutionListener(new TestListenerBeforeExecutionError())
            .registerExecutionListener(new TestListenerAfterExecutionError())
            .registerExecutionListener(new TestListenerAfterExceptionError());
    private final DataFlow dataFlow = new DataFlowBuilder()
            .withAnnotatedDataBuilder(TestBuilderA.class)
            .withAnnotatedDataBuilder(TestBuilderB.class)
            .withAnnotatedDataBuilder(TestBuilderC.class)
            .withTargetData("F")
            .build();
    private final DataFlow dataFlowError = new DataFlowBuilder()
            .withAnnotatedDataBuilder(TestBuilderError.class)
            .withTargetData("Y")
            .build();
    private final DataFlow dataFlowValidationError = new DataFlowBuilder()
            .withAnnotatedDataBuilder(TestBuilderDataValidationError.class)
            .withTargetData("Y")
            .build();
    private final DataFlow dataFlowValidationErrorWithPartialData = new DataFlowBuilder()
            .withAnnotatedDataBuilder(TestBuilderA.class)
            .withAnnotatedDataBuilder(TestBuilderDataValidationError.class)
            .withTargetData("Y")
            .build();

    @Test
    public void testRunThreeSteps() throws Exception {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(Lists.newArrayList(new TestDataA("Hello")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().isEmpty());
        }
        {
            val dataDelta = new DataDelta(Lists.newArrayList(new TestDataB("World")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertFalse(response.getResponses().isEmpty());
            assertTrue(response.getResponses().containsKey("C"));
        }
        {
            val dataDelta = new DataDelta(Lists.newArrayList(new TestDataD("this")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertFalse(response.getResponses().isEmpty());
            assertTrue(response.getResponses().containsKey("E"));
            assertTrue(response.getResponses().containsKey("F"));
        }
    }

    @Test
    public void testRunTwoSteps() throws Exception {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(Lists.newArrayList(new TestDataA("Hello"), new TestDataB("World")));
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
    public void testRunSingleStep() throws Exception {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(Lists.newArrayList(
                    new TestDataA("Hello"), new TestDataB("World"),
                    new TestDataD("this"), new TestDataG("Hmmm")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertEquals(3, response.getResponses().size());
            assertTrue(response.getResponses().containsKey("C"));
            assertTrue(response.getResponses().containsKey("E"));
            assertTrue(response.getResponses().containsKey("F"));
        }
    }

    @Test
    public void testRunError() throws Exception {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlowError);
        {
            val dataDelta = new DataDelta(Lists.newArrayList(new TestDataX("Hello")));
            try {
                executor.run(dataFlowInstance, dataDelta);
            }
            catch (Exception e) {
                assertEquals("TestError", e.getCause().getMessage());
                return;
            }
            fail("Should have thrown exception");
        }
    }

    @Test
    public void testRunErrorNPE() throws Exception {
        DataFlowInstance dataFlowInstance = new DataFlowInstance();
        dataFlowInstance.setId("testflow");
        dataFlowInstance.setDataFlow(dataFlowError);
        {
            DataDelta dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataX("Hello"), null));
            try {
                executor.run(dataFlowInstance, dataDelta);
            }
            catch (Exception e) {
                return;
            }
            fail("Should have thrown exception");
        }
    }

    @Test
    public void testRunValidationError() throws Exception {
        DataFlowInstance dataFlowInstance = new DataFlowInstance();
        dataFlowInstance.setId("testflow");
        dataFlowInstance.setDataFlow(dataFlowValidationError);
        {
            DataDelta dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataC("Hello")));
            try {
                executor.run(dataFlowInstance, dataDelta);
            }
            catch (Exception e) {
                assertEquals("DataValidationError", e.getCause().getMessage());
                return;
            }
            fail("Should have thrown exception");
        }
    }

    @Test
    public void testRunValidationErrorWithPartialData() throws Exception {
        DataFlowInstance dataFlowInstance = new DataFlowInstance();
        DataExecutionResponse response = new DataExecutionResponse();
        dataFlowInstance.setId("testflow");
        dataFlowInstance.setDataFlow(dataFlowValidationErrorWithPartialData);
        {
            DataDelta dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataA("Hello"),
                                                                         new TestDataB("World")));
            try {
                response = executor.run(dataFlowInstance, dataDelta);
            }
            catch (DataValidationException e) {
                DataExecutionResponse dataExecutionResponse = e.getResponse();
                assertTrue(dataExecutionResponse.getResponses().containsKey("C"));
                return;
            }
            fail("Should have thrown exception");
        }
    }


}
