package io.appform.databuilderframework;

import com.google.common.collect.Lists;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class DataExecutionListenerContextTest {

    private static final String KEY = "key";
    private static final String VALUE = "value";

    private static final class TestListenerWithContextCheck implements DataBuilderExecutionListener {
        private boolean allTestsPassed = true;

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
            try {
                Assert.assertNotNull(builderContext);
                assertEquals(VALUE, builderContext.getContextData(KEY, String.class));
            }
            catch (Exception e) {
                allTestsPassed = false;
                throw e;
            }
        }

        @Override
        public void afterExecute(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta, Map<String, Data> prevResponses, Data currentResponse) throws Exception {
            try {
                Assert.assertNotNull(builderContext);
                assertEquals(VALUE, builderContext.getContextData(KEY, String.class));
            }
            catch (Exception e) {
                allTestsPassed = false;
                throw e;
            }
        }

        @Override
        public void afterException(
                DataBuilderContext builderContext,
                DataFlowInstance dataFlowInstance,
                DataBuilderMeta builderToBeApplied,
                DataDelta dataDelta,
                Map<String, Data> prevResponses, Throwable frameworkException) throws Exception {
            try {
                Assert.assertNotNull(builderContext);
                assertEquals(VALUE, builderContext.getContextData(KEY, String.class));
            }
            catch (Exception e) {
                allTestsPassed = false;
                throw e;
            }
        }


        @Override
        public void postProcessing(
                DataFlowInstance dataFlowInstance,
                DataDelta dataDelta, DataExecutionResponse response,
                Throwable frameworkException) throws Exception {
            log.info("Being called for: {}", dataFlowInstance.getId());
        }
    }

    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager();
    private final DataFlowExecutor executor
            = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(dataBuilderMetadataManager));
    private final DataFlow dataFlow = new DataFlowBuilder()
            .withAnnotatedDataBuilder(TestBuilderA.class)
            .withAnnotatedDataBuilder(TestBuilderB.class)
            .withAnnotatedDataBuilder(TestBuilderC.class)
            .withTargetData("F")
            .build();
    private final TestListenerWithContextCheck listener = new TestListenerWithContextCheck();

    @Before
    public void setup() throws Exception {
        executor.registerExecutionListener(listener);
    }

    @Test
    public void testBuilderRunWithContextBeingPassed() throws Exception {
        val dataFlowInstance = new DataFlowInstance();
        dataFlowInstance.setId("testflow");
        dataFlowInstance.setDataFlow(dataFlow);
        {
            val context = new DataBuilderContext().saveContextData(KEY, VALUE);
            val dataDelta = new DataDelta(Lists.newArrayList(
                    new TestDataA("Hello"), new TestDataB("World"),
                    new TestDataD("this"), new TestDataG("Hmmm")));
            val response = executor.run(context, dataFlowInstance, dataDelta);
            assertEquals(3, response.getResponses().size());
            assertTrue(response.getResponses().containsKey("C"));
            assertTrue(response.getResponses().containsKey("E"));
            assertTrue(response.getResponses().containsKey("F"));
        }
        assertTrue(listener.allTestsPassed);
    }

}
