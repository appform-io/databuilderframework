package io.appform.databuilderframework.flowtest.tests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.appform.databuilderframework.engine.DataBuilderMetadataManager;
import io.appform.databuilderframework.engine.DataFlowExecutor;
import io.appform.databuilderframework.engine.ExecutionGraphGenerator;
import io.appform.databuilderframework.engine.SimpleDataFlowExecutor;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.flowtest.builders.*;
import io.appform.databuilderframework.flowtest.data.*;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Slf4j
public class FlowTest {
    private final DataBuilderMetadataManager dataBuilderMetadataManager
            = new DataBuilderMetadataManager()
            .register(ImmutableSet.of("CR", "CAID"), "OO", "OOB", OOBuilderTest.class)
            .register(ImmutableSet.of("OO", "OMSP"), "OCRD", "OCRDB", OCOBuilder.class)
            .register(ImmutableSet.of("OO", "CAID", "OMSP", "OCRD"), "POD", "PODB", POBuilder.class)
            .register(ImmutableSet.of("SPD", "OO", "OCRD", "POD"), "ILD", "ILDB", ILBuilder.class)
            .register(ImmutableSet.of("SPD", "ILD", "OO", "OCRD"), "IPD", "IPDB", IPBuilder.class)
            .register(ImmutableSet.of("IPD", "EPD", "OO"), "DPD", "DPDB", DPBuilder.class)
            .register(ImmutableSet.of("DPD", "IPD", "OO"), "OMSP", "OMSPD", OPBuilder.class)
            .register(ImmutableSet.of("OMSP", "OO", "DPD"), "PSD", "PSDB", PSBuilder.class)
            .register(ImmutableSet.of("PSD", "OMSP", "OO"), "PPD", "PPDB", PPBuilder.class)
            .register(ImmutableSet.of("PPD", "PSD"), "OCD", "OCDB", OCBuilder.class);

    private final DataFlowExecutor executor
            = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(dataBuilderMetadataManager));

    private final ExecutionGraphGenerator executionGraphGenerator = new ExecutionGraphGenerator(dataBuilderMetadataManager);

    private final ObjectMapper mapper = new ObjectMapper();

    public FlowTest() throws Exception {

        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    @Test
    public void testRunMultiStep() throws Exception {
        val dataFlow = new DataFlow()
                .setName("TestFlow")
                .setTargetData("OCD");

        val e = executionGraphGenerator.generateGraph(dataFlow);
        dataFlow.setExecutionGraph(e);

        val dataFlowInstance = new DataFlowInstance("Test", dataFlow, new DataSet());
        val executionGraph = dataFlowInstance.getDataFlow().getExecutionGraph();
        assertNotNull(executionGraph);
        val dependencyGraph = executionGraph.getDependencyHierarchy();
        assertEquals(10, dependencyGraph.size());

        {
            val dataDelta = new DataDelta(Lists.newArrayList(
                    new CAID(), new CR(), new OP()));
            val response = executor.run(dataFlowInstance, dataDelta);
            log.info(listPrint(response.getResponses().keySet()));
            assertEquals(3, response.getResponses().size());
        }
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(
                    new SPO()));
            val response = executor.run(dataFlowInstance, dataDelta);
            log.info(listPrint(response.getResponses().keySet()));
            assertEquals(2, response.getResponses().size());
        }
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(
                    new EPD()));
            val response = executor.run(dataFlowInstance, dataDelta);
            log.info(listPrint(response.getResponses().keySet()));
            assertEquals(5, response.getResponses().size());
        }
    }

    @Test
    public void testRunSingleStep() throws Exception {
        val dataFlow = new DataFlow();

        dataFlow.setName("TestFlow");
        dataFlow.setTargetData("OCD");

        val e = executionGraphGenerator.generateGraph(dataFlow);
        dataFlow.setExecutionGraph(e);

        val dataFlowInstance = new DataFlowInstance("Test", dataFlow, new DataSet());
        val executionGraph = dataFlowInstance.getDataFlow().getExecutionGraph();
        assertNotNull(executionGraph);
        val dependencyGraph = executionGraph.getDependencyHierarchy();
        assertEquals(10, dependencyGraph.size());

        {
            val dataDelta = new DataDelta(Lists.newArrayList(
                    new CAID(), new CR(), new OP(), new SPO(), new EPD()));
            val response = executor.run(dataFlowInstance, dataDelta);
            log.info(listPrint(response.getResponses().keySet()));
            assertEquals(10, response.getResponses().size());
        }
    }

    private String listPrint(Collection<String> dataset) {
        return Joiner.on(',').join(dataset);
    }

}
