package io.appform.databuilderframework.engine;

import com.google.common.collect.Maps;
import io.appform.databuilderframework.engine.util.TimedExecutor;
import io.appform.databuilderframework.model.DataBuilderMeta;
import io.appform.databuilderframework.model.DataFlow;
import io.appform.databuilderframework.model.ExecutionGraph;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class generates an {@link io.appform.databuilderframework.model.ExecutionGraph}.
 * It uses the target data and resolution spec provided in the {@link io.appform.databuilderframework.model.DataFlow}
 * to generate a dependency list. This is used later by the {@link DataFlowExecutor}
 * to run the flow.
 */
@Slf4j
public class ExecutionGraphGenerator {
    private final DataBuilderMetadataManager dataBuilderMetadataManager;

    public ExecutionGraphGenerator(DataBuilderMetadataManager dataBuilderMetadataManager) {
        this.dataBuilderMetadataManager = dataBuilderMetadataManager;
    }

    /**
     * Generates an {@link io.appform.databuilderframework.model.ExecutionGraph} for the given graph.
     * An exception is thrown if not target is specified, or there are multiple builders for the same data, but no
     * resolution is provided for the same(conflict).
     *
     * @param dataFlow The {@link io.appform.databuilderframework.model.DataFlow} object to be analyzed
     * @return Returns the ExecutionGraph
     * @throws DataBuilderFrameworkException
     */
    public ExecutionGraph generateGraph(final DataFlow dataFlow) throws DataBuilderFrameworkException {
        if (dataFlow.getTargetData() == null || dataFlow.getTargetData().isEmpty()) {
            throw new DataBuilderFrameworkException(
                    DataBuilderFrameworkException.ErrorCode.NO_TARGET_DATA, "No target data specified for flow");
        }
        val dependencyInfoManager = new DependencyInfoManager();

        /*
         * STEP 1:: GENERATE DEPENDENCY TREE {ROOT=>TARGET}
         */
        val root
                = TimedExecutor.run("ExecutionGraphGenerator::generateDependencyTree",
                                    () -> generateDependencyTree(dataFlow.getTargetData(), dataFlow, null,
                                                                 new DependencyNodeManager(), dependencyInfoManager,
                                                                 new FlattenedDataRoute()));
        /*
         * STEP 2:: RANK NODES IN THE TREE ACCORDING TO DISTANCE FROM ROOT
         */
        val maxHeight = TimedExecutor.run("ExecutionGraphGenerator::rankNodes",
                                          () -> rankNodes(root, 0, new HashSet<>()));

        /*
        STEP 3:: CREATE REPRESENTATION
        Representation : Example of a three level tree:
            A
            |
            +--B
            |  |
            |  +--C
            |  |
            |  +--D
            +--E
               |
               +--F
               |
               +--G
        Dependency Hierarchy:
        [
           0 : [C, D, F, G]
           1 : [B, E]
           2 : [A]
        ]
        */
        val dependencyHierarchy
                = TimedExecutor.run("ExecutionGraphGenerator::buildHierarchy",
                                    () -> buildHierarchy(dependencyInfoManager, maxHeight));

        //Return
        return new ExecutionGraph(dependencyHierarchy);
    }

    private List<List<DataBuilderMeta>> buildHierarchy(
            DependencyInfoManager dependencyInfoManager, int maxHeight) {
        Map<String, DependencyInfo> dependencyInfos = dependencyInfoManager.infos;

        //Fill up array with nulls. Array size == max Rank
        val dependencyHierarchy = new ArrayList<List<DataBuilderMeta>>(Collections.nCopies(maxHeight + 1, null));

        //For each dependency
        for (Map.Entry<String, DependencyInfo> dependencyInfo : dependencyInfos.entrySet()) {
            DataBuilderMeta tmpDataBuilderMeta
                    = dataBuilderMetadataManager.get(dependencyInfo.getValue().getBuilder());
            if (null == tmpDataBuilderMeta) {
                //Data is user-input data
                continue;
            }
            val rank = dependencyInfo.getValue().getRank();
            val dataBuilderMeta = tmpDataBuilderMeta.deepCopy().setRank(rank);

            //Set builder in the appropriate rank slots
            if (null == dependencyHierarchy.get(rank)) {
                dependencyHierarchy.set(rank, new ArrayList<>());
            }
            dependencyHierarchy.get(rank).add(dataBuilderMeta);
        }

        //A few levels will be null as they are built exclusively of user-input data
        //Remove these useless ranks
        dependencyHierarchy.removeAll(Collections.singleton(null));

        //Reverse the array for helping in bottom up traversal during execution
        Collections.reverse(dependencyHierarchy);
        return dependencyHierarchy;
    }

    private int rankNodes(DependencyNode root, int currentNode, Set<String> processedNodes) {
        val data = root.getData().getData();

        val currRank = root.getData().getRank();
        if (currRank >= currentNode && processedNodes.contains(data)) {
            return currentNode;
        }

        if (currRank < currentNode) {
            root.getData().setRank(currentNode);
        }

        val childNode = currentNode + 1;
        val ret = root.getIncoming()
                .stream()
                .mapToInt(child -> Math.max(rankNodes(child, childNode, processedNodes), childNode))
                .max()
                .orElse(childNode);
        processedNodes.add(data);
        return ret;
    }

    private DependencyNode generateDependencyTree(
            final String data, DataFlow dataFlow,m
            DependencyInfo outgoing,
            DependencyNodeManager dependencyNodeManager,
            DependencyInfoManager dependencyInfoManager,
            FlattenedDataRoute routeMeta) throws DataBuilderFrameworkException {
        val root = dependencyNodeManager.get(data);
        if (root.getData() != null) {
            log.debug("Precomputed dependency tree found for data: {}", data);
            return root;
        }
        log.debug("Generating dependency tree for: {}", data);
        val incoming = new ArrayList<DependencyNode>();
        val dataBuilderMeta = findBuilder(data, dataFlow);
        val info = dependencyInfoManager.get(data);
        if (null == info.getData()) {
            info.setData(data);
        }
        if (null != dataBuilderMeta) {
            if (null == info.getBuilder()) {
                info.setBuilder(dataBuilderMeta.getName());
            }
            for (String consumes : dataBuilderMeta.getEffectiveConsumes()) {
                if (routeMeta.isAlreadyOnOutgoingPath(data, consumes)) {
                    log.warn("Loop detected: Path for {} already contains {}", consumes, data);
                    continue;
                }
                routeMeta.addOutputData(consumes, data);
                incoming.add(generateDependencyTree(consumes, dataFlow, info,
                                                    dependencyNodeManager, dependencyInfoManager, routeMeta));
            }
        }
        root.setData(info);
        root.setIncoming(incoming);
        if (null != outgoing) {
            root.getOutgoing().add(outgoing);
        }

        return root;
    }

    private DataBuilderMeta findBuilder(String data, DataFlow dataFlow) throws DataBuilderFrameworkException {
        val resolutionSpecs = dataFlow.getResolutionSpecs();
        if (null != resolutionSpecs && resolutionSpecs.containsKey(data)) {
            val producerMeta = dataBuilderMetadataManager.get(resolutionSpecs.get(data));
            if (null == producerMeta) {
                //A resolution spec was specified but no builder was found
                throw new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.NO_BUILDER_FOR_DATA,
                                                        "No builder found with name: " + resolutionSpecs.get(data));
            }
            return producerMeta;
        }
        val producerMetaList = dataBuilderMetadataManager.getMetaForProducerOf(data);
        if (producerMetaList == null) {
            log.debug("Starting data point found: {}", data);
            return null;
        }

        if (producerMetaList.size() > 1) {
            //No resolution spec was specified, but multiple builders were found
            log.error("Multiple builders found for data, but no resolution spec found. Cannot proceed. data: {}", data);
            throw new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.BUILDER_RESOLUTION_CONFLICT_FOR_DATA,
                                                    "Multiple builders found for data, but no resolution spec found. Cannot proceed. Data: " + data);
        }
        return producerMetaList.get(0);
    }

    private static class FlattenedDataRoute {
        private final Map<String, Set<String>> outgoingMap = new ConcurrentHashMap<>();

        public void addOutputData(String input, String output) {
            outgoingMap.computeIfAbsent(input, k -> new HashSet<>())
                    .add(input);
            if (outgoingMap.containsKey(output)) {
                outgoingMap.get(input).addAll(outgoingMap.get(output)); //My output's outputs are my outputs also
            }
        }

        public boolean isAlreadyOnOutgoingPath(String input, String output) {
            if (!outgoingMap.containsKey(input)) {
                return false;
            }
            return outgoingMap.get(input).contains(output);
        }
    }

    @Data
    private static class DependencyInfo {
        private String data;
        private String builder;
        private int rank = 0;
    }

    private static final class DependencyInfoManager {
        private final Map<String, DependencyInfo> infos = new ConcurrentHashMap<>();

        DependencyInfo get(String data) {
            return infos.computeIfAbsent(data, k -> new DependencyInfo());
        }
    }

    @Data
    private static class DependencyNode {
        private DependencyInfo data;
        private List<DependencyNode> incoming = new ArrayList<>();
        private Set<DependencyInfo> outgoing = new LinkedHashSet<>();
    }

    private static class DependencyNodeManager {
        private final Map<String, DependencyNode> dataList = Maps.newHashMap();

        DependencyNode get(String data) {
            return dataList.computeIfAbsent(data, k -> new DependencyNode());
        }
    }
}
