package io.appform.databuilderframework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
public class DataFlowWithTransientDataTest {

    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager()
            .register(ImmutableSet.of("A", "B"), "C", "BuilderA", TestBuilderA.class )
            .register(ImmutableSet.of("C", "D"), "E", "BuilderB", TestBuilderB.class )
            .register(ImmutableSet.of("A", "E"), "F", "BuilderC", TestBuilderC.class )
            .register(ImmutableSet.of("X"), "Y", "BuilderX", TestBuilderError.class);
    private final DataFlowExecutor executor = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(dataBuilderMetadataManager));
    private final ExecutionGraphGenerator executionGraphGenerator = new ExecutionGraphGenerator(dataBuilderMetadataManager);
    private final DataFlow dataFlow = new DataFlow()
            .setTargetData("F")
            .setTransients(Sets.newHashSet("C"));

    @Before
    public void setup() throws Exception {
        dataFlow.setExecutionGraph(executionGraphGenerator.generateGraph(dataFlow).deepCopy());
        log.info("{}",new ObjectMapper().writeValueAsString(dataFlow));
    }

    @Test
    public void testTransient() throws Exception {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataA("Hello")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().isEmpty());
        }
        Data dataC = null;
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataB("World")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertFalse(response.getResponses().isEmpty());
            assertTrue(response.getResponses().containsKey("C"));
            val accessor = new DataSetAccessor(dataFlowInstance.getDataSet());
            assertTrue(accessor.checkForData(ImmutableSet.of("A", "B")));
            assertFalse(accessor.checkForData("C"));
            dataC = response.getResponses().get("C");
        }
        {
            val dataDelta = new DataDelta(Lists.newArrayList(new TestDataD("this"), dataC));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertFalse(response.getResponses().isEmpty());
            assertTrue(response.getResponses().containsKey("E"));
            assertTrue(response.getResponses().containsKey("F"));
        }
    }
}
