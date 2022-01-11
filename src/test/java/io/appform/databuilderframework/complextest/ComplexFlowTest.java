package io.appform.databuilderframework.complextest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.appform.databuilderframework.engine.DataBuilderMetadataManager;
import io.appform.databuilderframework.engine.DataFlowExecutor;
import io.appform.databuilderframework.engine.ExecutionGraphGenerator;
import io.appform.databuilderframework.engine.SimpleDataFlowExecutor;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ComplexFlowTest {
    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager()
            .register(ImmutableSet.of("CR", "CAID", "VAS"), "OO", "SB", SB.class)
            .register(ImmutableSet.of("OO"), "POD", "POB", POB.class)
            .register(ImmutableSet.of("OO", "POD", "SPD"), "ILD", "PRB", PRB.class)
            .register(ImmutableSet.of("ILD", "EPD"), "OSD", "PPB", PPB.class)
            .register(ImmutableSet.of("OO", "OSD"), "OCD", "COB", COB.class);

    private final DataFlowExecutor executor = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(
            dataBuilderMetadataManager));

    private final ExecutionGraphGenerator executionGraphGenerator = new ExecutionGraphGenerator(dataBuilderMetadataManager);


    @Test
    public void testRunWeb() throws Exception {
        DataFlow dataFlow = new DataFlow();

        dataFlow.setName("TestFlow");
        dataFlow.setTargetData("OCD");

        ExecutionGraph e = executionGraphGenerator.generateGraph(dataFlow);
        dataFlow.setExecutionGraph(e);

        DataFlowInstance complexFlowIntsnace = new DataFlowInstance("Test", dataFlow, new DataSet());
        {
            DataDelta dataDelta = new DataDelta(Lists.newArrayList(new CR(), new CAID(), new VAS()));
            DataExecutionResponse response = executor.run(complexFlowIntsnace, dataDelta);
            log.info(new ObjectMapper().writeValueAsString(response));
            val responses = response.getResponses();
            assertEquals(2, responses.size());
            assertTrue(responses.containsKey("OO"));
            assertTrue(responses.containsKey("POD"));
        }
        {
            DataDelta dataDelta = new DataDelta(Lists.<Data>newArrayList(new SPD()));
            DataExecutionResponse response = executor.run(complexFlowIntsnace, dataDelta);
            log.info(new ObjectMapper().writeValueAsString(response));
            val responses = response.getResponses();
            assertEquals(1, responses.size());
            assertTrue(responses.containsKey("ILD"));
        }
        {
            DataDelta dataDelta = new DataDelta(Lists.<Data>newArrayList(new EPD()));
            DataExecutionResponse response = executor.run(complexFlowIntsnace, dataDelta);
            log.info(new ObjectMapper().writeValueAsString(response));
            val responses = response.getResponses();
            assertEquals(2, responses.size());
            assertTrue(responses.containsKey("OCD"));
            assertTrue(responses.containsKey("OSD"));
        }

    }

    @Test
    public void testRunMobile() throws Exception {
        DataFlow dataFlow = new DataFlow();

        dataFlow.setName("TestFlow");
        dataFlow.setTargetData("OCD");

        ExecutionGraph e = executionGraphGenerator.generateGraph(dataFlow);
        dataFlow.setExecutionGraph(e);

        DataFlowInstance complexFlowIntsnace = new DataFlowInstance("Test", dataFlow, new DataSet());
        {
            val dataDelta = new DataDelta(Lists.newArrayList(new CR(), new CAID(), new VAS(), new SPD()));
            val response = executor.run(complexFlowIntsnace, dataDelta);
            log.info(new ObjectMapper().writeValueAsString(response));
            val responses = response.getResponses();
            assertEquals(3, responses.size());
            assertTrue(responses.containsKey("OO"));
            assertTrue(responses.containsKey("POD"));
            assertTrue(responses.containsKey("ILD"));

        }
        {
            DataDelta dataDelta = new DataDelta(Lists.<Data>newArrayList(new EPD()));
            DataExecutionResponse response = executor.run(complexFlowIntsnace, dataDelta);
            log.info(new ObjectMapper().writeValueAsString(response));
            val responses = response.getResponses();
            assertEquals(2, responses.size());
            assertTrue(responses.containsKey("OCD"));
            assertTrue(responses.containsKey("OSD"));
        }

    }

    @Test
    public void testRunSubscription() throws Exception {
        DataFlow dataFlow = new DataFlow();

        dataFlow.setName("TestFlow");
        dataFlow.setTargetData("OCD");

        ExecutionGraph e = executionGraphGenerator.generateGraph(dataFlow);
        dataFlow.setExecutionGraph(e);

        DataFlowInstance complexFlowIntsnace = new DataFlowInstance("Test", dataFlow, new DataSet());
        {
            DataDelta dataDelta = new DataDelta(Lists.newArrayList(new CR(),
                                                                   new CAID(),
                                                                   new VAS(),
                                                                   new SPD(),
                                                                   new EPD()));
            DataExecutionResponse response = executor.run(complexFlowIntsnace, dataDelta);
            log.info(new ObjectMapper().writeValueAsString(response));
            val responses = response.getResponses();
            assertEquals(5, responses.size());
            assertTrue(responses.containsKey("OCD"));
            assertTrue(responses.containsKey("OSD"));
            assertTrue(responses.containsKey("ILD"));
            assertTrue(responses.containsKey("OO"));
            assertTrue(responses.containsKey("POD"));
        }

    }

}
