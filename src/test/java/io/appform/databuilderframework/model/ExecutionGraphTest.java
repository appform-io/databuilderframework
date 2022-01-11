package io.appform.databuilderframework.model;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import lombok.val;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ExecutionGraphTest {
    @Test
    public void testDeepCopyEmpty() throws Exception {
        val executionGraph = new ExecutionGraph(Collections.<List<DataBuilderMeta>>emptyList());
        val executionGraph1 = executionGraph.deepCopy();
        assertEquals(0, executionGraph1.getDependencyHierarchy().size());
    }

    @Test
    public void testDeepCopy() throws Exception {
        val builders = Lists.newArrayList(
                                            new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test"));
        val executionGraph = new ExecutionGraph(Collections.singletonList(builders));
        val executionGraph1 = executionGraph.deepCopy();
        assertArrayEquals(executionGraph.getDependencyHierarchy().get(0).toArray(new DataBuilderMeta[executionGraph.getDependencyHierarchy().size()]),
                executionGraph1.getDependencyHierarchy().get(0).toArray(new DataBuilderMeta[executionGraph.getDependencyHierarchy().size()]));
    }
}
