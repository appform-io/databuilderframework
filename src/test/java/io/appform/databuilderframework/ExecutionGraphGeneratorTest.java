package io.appform.databuilderframework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.appform.databuilderframework.engine.DataBuilderFrameworkException;
import io.appform.databuilderframework.engine.DataBuilderMetadataManager;
import io.appform.databuilderframework.engine.DataFlowBuilder;
import io.appform.databuilderframework.engine.ExecutionGraphGenerator;
import io.appform.databuilderframework.model.DataFlow;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

@Slf4j
public class ExecutionGraphGeneratorTest {
    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager()
            .register(ImmutableSet.of("A", "B"), "C", "BuilderA", TestBuilderA.class)
            .register(ImmutableSet.of("C", "D"), "E", "BuilderB", TestBuilderB.class)
            .register(ImmutableSet.of("C", "E"), "F", "BuilderC", TestBuilderC.class)
            .register(ImmutableSet.of("F"), "G", "BuilderD", TestBuilderD.class)
            .register(ImmutableSet.of("E", "C"), "G", "BuilderE", TestBuilderE.class);
    private final ExecutionGraphGenerator executionGraphGenerator = new ExecutionGraphGenerator(
            dataBuilderMetadataManager);


    @Test
    public void testGenerateGraphNoTarget() throws Exception {
        val dataFlow = new DataFlow()
                .setName("test");
        try {
            executionGraphGenerator.generateGraph(dataFlow);
        }
        catch (DataBuilderFrameworkException e) {
            assertEquals(DataBuilderFrameworkException.ErrorCode.NO_TARGET_DATA, e.getErrorCode());
        }
        catch (Exception e) {
            log.error("Error raised: ", e);
            fail("Unexpected error");
        }
    }

    @Test
    public void testGenerateGraphEmptyTarget() throws Exception {
        val dataFlow = new DataFlow()
                .setName("test")
                .setTargetData("");
        try {
            executionGraphGenerator.generateGraph(dataFlow);
        }
        catch (DataBuilderFrameworkException e) {
            assertEquals(DataBuilderFrameworkException.ErrorCode.NO_TARGET_DATA, e.getErrorCode());
        }
        catch (Exception e) {
            log.error("Error raised: ", e);
            fail("Unexpected error");
        }
    }

    @Test
    public void testGenerateGraphNoExecutors() throws Exception {
        val dataFlow = new DataFlowBuilder()
                .withMetaDataManager(dataBuilderMetadataManager)
                .withName("test")
                .withTargetData("X")
                .build();
        val e = dataFlow.getExecutionGraph();
        assertTrue(e.getDependencyHierarchy().isEmpty());
    }

    @Test
    public void testGenerate() throws Exception {
        val dataFlow = new DataFlowBuilder()
                .withMetaDataManager(dataBuilderMetadataManager)
                .withName("test")
                .withTargetData("C")
                .build();
        val e = dataFlow.getExecutionGraph();
        assertFalse(e.getDependencyHierarchy().isEmpty());
        assertEquals(1, e.getDependencyHierarchy().size());
        assertEquals("BuilderA", e.getDependencyHierarchy().get(0).get(0).getName());
    }

    @Test
    public void testGenerateTwoStep() throws Exception {
        val dataFlow = new DataFlowBuilder()
                .withMetaDataManager(dataBuilderMetadataManager)
                .withName("test")
                .withTargetData("E")
                .build();
        val e = dataFlow.getExecutionGraph();
        assertFalse(e.getDependencyHierarchy().isEmpty());
        assertEquals(2, e.getDependencyHierarchy().size());
        assertEquals("BuilderA", e.getDependencyHierarchy().get(0).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(0).size());
        assertEquals("BuilderB", e.getDependencyHierarchy().get(1).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(1).size());
    }

    @Test
    public void testGenerateInterdependentStep() throws Exception {
        val dataFlow = new DataFlowBuilder()
                .withMetaDataManager(dataBuilderMetadataManager)
                .withName("test")
                .withTargetData("F")
                .build();
        val e = dataFlow.getExecutionGraph();
        assertEquals(3, e.getDependencyHierarchy().size());
        assertEquals("BuilderA", e.getDependencyHierarchy().get(0).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(0).size());
        assertEquals("BuilderB", e.getDependencyHierarchy().get(1).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(1).size());
        assertEquals("BuilderC", e.getDependencyHierarchy().get(2).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(2).size());
    }

    @Test
    public void testGenerateInterdependentStepConflict() throws Exception {
        try {
            new DataFlowBuilder()
                    .withMetaDataManager(dataBuilderMetadataManager)
                    .withName("test")
                    .withTargetData("G")
                    .build();
        }
        catch (DataBuilderFrameworkException e) {
            assertEquals(DataBuilderFrameworkException.ErrorCode.BUILDER_RESOLUTION_CONFLICT_FOR_DATA,
                         e.getErrorCode());
            return;
        }
        fail("A conflict should have come here");
    }

    @Test
    public void testGenerateInterdependentStepConflictNoData() throws Exception {
        val dataFlow = new DataFlow()
                .setName("test")
                .setTargetData("G")
                .setResolutionSpecs(Collections.singletonMap("G", "aa"));
        try {
            executionGraphGenerator.generateGraph(dataFlow);
        }
        catch (DataBuilderFrameworkException e) {
            assertEquals(DataBuilderFrameworkException.ErrorCode.NO_BUILDER_FOR_DATA, e.getErrorCode());
            return;
        }
        fail("A conflict should have come here");
    }

    @Test
    public void testGenerateInterdependentStepWithResolution() throws Exception {
        val dataFlow = new DataFlowBuilder()
                .withMetaDataManager(dataBuilderMetadataManager)
                .withName("test")
                .withTargetData("G")
                .withResolutionSpec("G", "BuilderE")
                .build();
        val e = dataFlow.getExecutionGraph();
        assertEquals(3, e.getDependencyHierarchy().size());
        assertEquals("BuilderA", e.getDependencyHierarchy().get(0).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(0).size());
        assertEquals("BuilderB", e.getDependencyHierarchy().get(1).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(1).size());
        assertEquals("BuilderE", e.getDependencyHierarchy().get(2).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(2).size());

    }

    @Test
    public void testGenerateInterdependentStepWithResolutionAlt() throws Exception {
        val dataFlow = new DataFlowBuilder()
                .withMetaDataManager(dataBuilderMetadataManager)
                .withName("test")
                .withTargetData("G")
                .withResolutionSpec("G", "BuilderD")
                .build();
        val e = dataFlow.getExecutionGraph();
        log.info("{}", new ObjectMapper().writeValueAsString(e));
        assertEquals(4, e.getDependencyHierarchy().size());
        assertEquals("BuilderA", e.getDependencyHierarchy().get(0).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(0).size());
        assertEquals("BuilderB", e.getDependencyHierarchy().get(1).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(1).size());
        assertEquals("BuilderC", e.getDependencyHierarchy().get(2).get(0).getName());
        assertEquals(1, e.getDependencyHierarchy().get(2).size());
        assertEquals("BuilderD", e.getDependencyHierarchy().get(3).get(0).getName());

    }

}
