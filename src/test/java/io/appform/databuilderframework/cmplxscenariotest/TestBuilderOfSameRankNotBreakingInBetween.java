package io.appform.databuilderframework.cmplxscenariotest;

import com.google.common.collect.Sets;
import io.appform.databuilderframework.cmplxscenariotest.builders.*;
import io.appform.databuilderframework.cmplxscenariotest.data.DataA;
import io.appform.databuilderframework.cmplxscenariotest.data.InputAData;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.DataFlow;
import io.appform.databuilderframework.model.DataFlowInstance;
import lombok.val;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

public class TestBuilderOfSameRankNotBreakingInBetween {

    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager()
            .register(BuilderA1.class)
            .register(BuilderA2.class)
            .register(BuilderA3.class)
            .register(BuilderB1.class)
            .register(BuilderB2.class)
            .register(BuilderB3.class)
            .register(BuilderB4.class)
            .register(BuilderB5.class)
            .register(BuilderC.class)
            .register(BuilderD.class)
            .register(BuilderE1.class)
            .register(BuilderE2.class)
            .register(BuilderE3.class)
            .register(BuilderE4.class)
            .register(BuilderE5.class)
            .register(BuilderE6.class)
            .register(BuilderF.class)
            .register(BuilderG.class)
            .register(BuilderH.class)
            .register(BuilderI.class)
            .register(BuilderJ.class)
            .register(BuilderK.class);
    private final SimpleDataFlowExecutor se = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(
            dataBuilderMetadataManager));
    //	private final ProfileExecutor builderExecutor = new ProfileExecutor(200, 250);
    private final ExecutorService builderExecutor = Executors.newFixedThreadPool(200);
    private final MultiThreadedDataFlowExecutor me
            = new MultiThreadedDataFlowExecutor(new InstantiatingDataBuilderFactory(dataBuilderMetadataManager), builderExecutor);

    @Test
    public void testToCheckIfBuilderA2RunsEvenWhenRankIsLess() throws DataBuilderFrameworkException, DataValidationException {
        val dataflow = new DataFlow()
                .setDescription("Complex DataFlow")
                .setEnabled(true)
                .setTargetData("K")
                .setTransients(Sets.newHashSet("IA", "I"))
                .setName("complext_flow");

        val graphGenerator = new ExecutionGraphGenerator(dataBuilderMetadataManager);
        val graph = graphGenerator.generateGraph(dataflow);
        dataflow.setExecutionGraph(graph);

        val instance = new DataFlowInstance("test", dataflow);
        val resp = se.run(instance, new DataA(8), new InputAData());
        assertTrue(resp.getResponses().containsKey("K"));

        val meInstance = new DataFlowInstance("test2", dataflow);
        val resp2 = me.run(meInstance, new DataA(8), new InputAData());
        assertTrue(resp2.getResponses().containsKey("K"));
    }
}
