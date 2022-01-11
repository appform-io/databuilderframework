package io.appform.databuilderframework;

import com.google.common.collect.Lists;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class PerfWithAccessesTest {
    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager();
    private final DataFlowExecutor executor = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(dataBuilderMetadataManager));
    private final DataFlow dataFlow = new DataFlowBuilder()
            .withAnnotatedDataBuilder(TestBuilderWithoutAccesses.class)
            .withTargetData("X")
            .build();
    private final DataFlow dataFlowWithAccesses = new DataFlowBuilder()
            .withAnnotatedDataBuilder(TestBuilderAccesses.class)
            .withTargetData("X")
            .build();

    @Test
    public void shouldBeComparableWithoutAnyAccesibleData() throws DataBuilderFrameworkException, DataValidationException {
        val simpleTime = executeFlow(dataFlow);
        val accessTime = executeFlow(dataFlowWithAccesses);
        val simpleAccessTime = executeFlow(dataFlowWithAccesses);

        log.info("Time without Accesses : {} ms Time with Accesses : {} ms Time with accesses w/o data : {} ms ",
                 simpleTime, accessTime, simpleAccessTime);
    }

    private long executeFlow(final DataFlow dataFlow) throws DataValidationException {
        long executionTime =  0L;
        for (int i = 0; i < 10000; i++) {
            val dataFlowInstance = new DataFlowInstance()
                    .setId("testflow")
                    .setDataFlow(dataFlow);
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataA("HEy")));
            val startTime = System.currentTimeMillis();
            val response = executor.run(dataFlowInstance, dataDelta);
            executionTime += (System.currentTimeMillis() - startTime);
            Assert.assertEquals(1, response.getResponses().size());
        }
        return executionTime;
    }


}
