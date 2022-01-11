package io.appform.databuilderframework.cmplxscenariotest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Sets;
import io.appform.databuilderframework.cmplxscenariotest.builders.*;
import io.appform.databuilderframework.engine.DataBuilderMetadataManager;
import io.appform.databuilderframework.engine.ExecutionGraphGenerator;
import io.appform.databuilderframework.model.DataBuilderMeta;
import io.appform.databuilderframework.model.DataFlow;
import io.appform.databuilderframework.model.ExecutionGraph;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@Slf4j
public class ExecutionGraphGeneratorTest {
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

    @Test
    public void testGraphGeneration() throws Exception {

        val dataflow = new DataFlow()
                .setDescription("Complex DataFlow")
                .setEnabled(true)
                .setTargetData("K")
                .setTransients(Sets.newHashSet("IA", "I"))
                .setName("complext_flow");

        val graphGenerator = new ExecutionGraphGenerator(dataBuilderMetadataManager);
        val graph = graphGenerator.generateGraph(dataflow);
        dataflow.setExecutionGraph(graph);
        val mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        log.info(mapper.writeValueAsString(dataflow));
        val expected = mapper.readValue(
                Files.readAllBytes(Paths.get(Objects.requireNonNull(this.getClass()
                                                                            .getResource("/execgraphgentest.json"))
                                                     .toURI())),
                ExecutionGraph.class);
        assertEquals(
                expected.getDependencyHierarchy()
                        .stream()
                        .flatMap(Collection::stream)
                        .map(DataBuilderMeta::getName)
                        .sorted()
                        .collect(Collectors.toList()),
                graph.getDependencyHierarchy()
                        .stream()
                        .flatMap(Collection::stream)
                        .map(DataBuilderMeta::getName)
                        .sorted()
                        .collect(Collectors.toList()));
        assertEquals(
                expected.getDependencyHierarchy()
                        .stream()
                        .flatMap(Collection::stream)
                        .sorted(Comparator.comparing(DataBuilderMeta::getName))
                        .map(DataBuilderMeta::getRank)
                        .collect(Collectors.toList()),
                graph.getDependencyHierarchy()
                        .stream()
                        .flatMap(Collection::stream)
                        .sorted(Comparator.comparing(DataBuilderMeta::getName))
                        .map(DataBuilderMeta::getRank)
                        .collect(Collectors.toList()));
    }

}
