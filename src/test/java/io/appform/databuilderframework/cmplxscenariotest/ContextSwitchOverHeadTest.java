package io.appform.databuilderframework.cmplxscenariotest;

import com.google.common.collect.Sets;
import io.appform.databuilderframework.cmplxscenariotest.builders.*;
import io.appform.databuilderframework.cmplxscenariotest.data.DataA;
import io.appform.databuilderframework.cmplxscenariotest.data.InputAData;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.DataExecutionResponse;
import io.appform.databuilderframework.model.DataFlow;
import io.appform.databuilderframework.model.DataFlowInstance;
import io.appform.databuilderframework.model.ExecutionGraph;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;

@Slf4j
@Ignore
public class ContextSwitchOverHeadTest {
    private final DataBuilderMetadataManager dataBuilderMetadataManager
            = new DataBuilderMetadataManager()
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

    private final DataFlow dataflow = new DataFlow()
            .setDescription("Complex DataFlow")
            .setEnabled(true)
            .setTargetData("K")
            .setTransients(Sets.newHashSet("IA", "I"))
            .setName("complext_flow");

    @Before
    public void setup() throws Exception {
        ExecutionGraphGenerator graphGenerator = new ExecutionGraphGenerator(dataBuilderMetadataManager);
        ExecutionGraph graph = graphGenerator.generateGraph(dataflow);
        dataflow.setExecutionGraph(graph);
    }

    @Test
    public void testWhenConccurencyIsLessThanBuilderSize() throws Exception {
        runTestWithUnBoundedQueue(150, 100);
    }

    @Test
    public void testWhenConccurencyIsMoreThanBuilderSize() throws Exception {
        log.info("testWhenConccurencyIsMoreThanBuilderSize");
        runTestWithUnBoundedQueue(50, 150);
    }

    @Test
    public void testWhenConccurencyIsSameAsBuilderSize() throws Exception {
        log.info("testWhenConccurencyIsSameAsBuilderSize");
        runTestWithUnBoundedQueue(150, 150);
    }

    public void runTestWithUnBoundedQueue(int builderThreadSize, int concurrentRequestSize) throws Exception {

        log.info("running unbounded queue test with builder size {} and concurrecy {}",
                 builderThreadSize,
                 concurrentRequestSize);
        val se = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(
                dataBuilderMetadataManager));
        val builderExecutor = new ProfileExecutor(builderThreadSize, -1, 50);
        val me = new MultiThreadedDataFlowExecutor(new InstantiatingDataBuilderFactory(
                dataBuilderMetadataManager), builderExecutor);

        val ome = new OptimizedMultiThreadedDataFlowExecutor(new InstantiatingDataBuilderFactory(
                dataBuilderMetadataManager), builderExecutor);

        val dataflowRef = dataflow;

        val exec = Executors.newFixedThreadPool(concurrentRequestSize); // concurrent users
        long start = System.currentTimeMillis();
        val failed = new AtomicBoolean();
        for (int i = 0; i < 1000; i++) {
            exec.execute(() -> {
                try {
                    val instance = new DataFlowInstance("test", dataflowRef);
                    val resp = se.run(instance, new DataA(), new InputAData());
                    failed.set(resp.getResponses().containsKey("K"));
                }
                catch (DataBuilderFrameworkException | DataValidationException e) {
                    log.error("Error during execution", e);
                    failed.set(true);
                }
            });
        }
        exec.shutdown();
        exec.awaitTermination(200, TimeUnit.SECONDS);
        log.info("se {}", (System.currentTimeMillis() - start));
        assertFalse(failed.get());

        val newExec = Executors.newFixedThreadPool(concurrentRequestSize); // concurrent users
        start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            newExec.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        final DataFlowInstance instance = new DataFlowInstance("test", dataflowRef);
                        DataExecutionResponse resp = me.run(instance, new DataA(), new InputAData());
                        failed.set(resp.getResponses().containsKey("K"));

                    }
                    catch (DataBuilderFrameworkException | DataValidationException e) {
                        log.error("Error during execution", e);
                        failed.set(true);
                    }
                }
            });
        }
        newExec.shutdown();
        newExec.awaitTermination(200, TimeUnit.SECONDS);
        log.info("me {}", (System.currentTimeMillis() - start));
        log.info("me run: context switches over threshold count {}, max latent switch: {}, rejections {}",
                 builderExecutor.getNumberOfContextSwitchesOverThresHold(),
                 builderExecutor.getMaxContextSwitchLatency(),
                 builderExecutor.getRejectedCount());
        builderExecutor.resetStats();
        assertFalse(failed.get());

        val omeExec = Executors.newFixedThreadPool(concurrentRequestSize); // concurrent users
        start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            omeExec.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        final DataFlowInstance instance = new DataFlowInstance("test", dataflowRef);
                        DataExecutionResponse resp = ome.run(instance, new DataA(), new InputAData());
                        failed.set(resp.getResponses().containsKey("K"));
                    }
                    catch (DataBuilderFrameworkException | DataValidationException e) {
                        log.error("Error during execution", e);
                        failed.set(true);
                    }
                }
            });
        }
        omeExec.shutdown();
        omeExec.awaitTermination(200, TimeUnit.SECONDS);
        log.info("ome {}" + (System.currentTimeMillis() - start));
        log.info("ome run: context switches over threshold count {}, max latent switch: {}, rejections {}",
                 builderExecutor.getNumberOfContextSwitchesOverThresHold(),
                 builderExecutor.getMaxContextSwitchLatency(),
                 builderExecutor.getRejectedCount());
        builderExecutor.resetStats();
        builderExecutor.shutdownNow();
        assertFalse(failed.get());
    }

}
