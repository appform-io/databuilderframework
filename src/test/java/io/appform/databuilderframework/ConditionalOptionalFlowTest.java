package io.appform.databuilderframework;


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.*;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConditionalOptionalFlowTest {
    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager()
            .register(ImmutableSet.of("A", "B"), "C", "BuilderA", TestBuilderA.class) //concats A and B values
            .registerWithOptionals(ImmutableSet.of("C", "D"),
                                   ImmutableSet.of("G"),
                                   "E",
                                   "BuilderB",
                                   ConditionalBuilder.class)
            .register(ImmutableSet.of("A", "E"), "F", "BuilderC", TestBuilderC.class);

    private final DataFlowExecutor executor = new SimpleDataFlowExecutor(new InstantiatingDataBuilderFactory(
            dataBuilderMetadataManager));

    private final DataFlow dataFlow = new DataFlowBuilder()
            .withMetaDataManager(dataBuilderMetadataManager)
            .withTargetData("F")
            .build();

    // conditional builder here runs if C and D are present.
    // if C == "Hello World"
    // and D == "this" or G is present and G == this
    public static final class ConditionalBuilder extends DataBuilder {

        @Override
        public Data process(DataBuilderContext context) throws DataBuilderException {
            val accessor = new DataSetAccessor(context.getDataSet());
            val dataC = accessor.get("C", TestDataC.class);
            val dataD = accessor.get("D", TestDataD.class);
            val isGPresent = accessor.checkForData("G");

            if (dataC.getValue().equals("Hello World")) {
                if (dataD.getValue().equalsIgnoreCase("this")) {
                    return new TestDataE("Wah wah!!");
                }
                else if (isGPresent) {
                    TestDataG dataG = accessor.get("G", TestDataG.class);
                    if (dataG.getValue().equalsIgnoreCase("this")) {
                        return new TestDataE("Wah wah!!");
                    }
                }

            }
            return null;
        }
    }

    @Test
    public void testRunWithOptional() throws Exception {
        val dataFlowInstance = new DataFlowInstance()
                .setId("testflow")
                .setDataFlow(dataFlow);
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataA("Hello"),
                                                                         new TestDataB("World"),
                                                                         new TestDataD("notThis")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().containsKey("C"));
            assertFalse(response.getResponses().containsKey("E"));
        }
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataG("notThis")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertFalse(response.getResponses().containsKey("E"));
            assertFalse(response.getResponses().containsKey("C"));
        }
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataG("this")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().containsKey("E"));
            assertTrue(response.getResponses().containsKey("F"));
        }
        {
            val dataDelta = new DataDelta(Lists.<Data>newArrayList(new TestDataD("this")));
            val response = executor.run(dataFlowInstance, dataDelta);
            assertTrue(response.getResponses().containsKey("E"));
            assertTrue(response.getResponses().containsKey("F"));
        }

    }

}
