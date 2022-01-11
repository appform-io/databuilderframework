package io.appform.databuilderframework.complextest;

import com.google.common.collect.ImmutableSet;
import io.appform.databuilderframework.TestBuilderA;
import io.appform.databuilderframework.TestBuilderB;
import io.appform.databuilderframework.TestBuilderC;
import io.appform.databuilderframework.TestBuilderD;
import io.appform.databuilderframework.engine.DataBuilderMetadataManager;
import io.appform.databuilderframework.engine.ExecutionGraphGenerator;
import io.appform.databuilderframework.model.DataFlow;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExampleTest {

    @Test
    public void testBalancedTree() throws Exception {
        val dataBuilderMetadataManager = new DataBuilderMetadataManager()
                .register(ImmutableSet.of("A", "B"), "C", "BuilderA", TestBuilderA.class)
                .register(ImmutableSet.of("D", "E"), "F", "BuilderC", TestBuilderB.class)
                .register(ImmutableSet.of("C", "F"), "G", "BuilderD", TestBuilderC.class);
        val executionGraphGenerator = new ExecutionGraphGenerator(dataBuilderMetadataManager);
        val dataFlow = new DataFlow()
                .setName("test")
                .setTargetData("G");
        val e = executionGraphGenerator.generateGraph(dataFlow);
        assertEquals(2, e.getDependencyHierarchy().size());
        assertEquals(2, e.getDependencyHierarchy().get(0).size());
        assertEquals(1, e.getDependencyHierarchy().get(1).size());
    }

    @Test
    public void testDiamond() throws Exception {
        val dataBuilderMetadataManager = new DataBuilderMetadataManager()
                .register(ImmutableSet.of("A", "B"), "C", "BuilderA", TestBuilderA.class)
                .register(ImmutableSet.of("C", "E"), "F", "BuilderB", TestBuilderB.class)
                .register(ImmutableSet.of("C", "G"), "H", "BuilderC", TestBuilderC.class)
                .register(ImmutableSet.of("F", "H"), "X", "BuilderD", TestBuilderD.class);
        val executionGraphGenerator = new ExecutionGraphGenerator(dataBuilderMetadataManager);
        val dataFlow = new DataFlow()
                .setName("test")
                .setTargetData("X");
        val e = executionGraphGenerator.generateGraph(dataFlow);
        assertEquals(3, e.getDependencyHierarchy().size());
        assertEquals(1, e.getDependencyHierarchy().get(0).size());
        assertEquals(2, e.getDependencyHierarchy().get(1).size());
        assertEquals(1, e.getDependencyHierarchy().get(2).size());
    }

}
