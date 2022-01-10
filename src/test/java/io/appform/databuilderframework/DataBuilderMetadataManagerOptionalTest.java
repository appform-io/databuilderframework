package io.appform.databuilderframework;

import com.google.common.collect.ImmutableSet;
import io.appform.databuilderframework.engine.DataBuilderFrameworkException;
import io.appform.databuilderframework.engine.DataBuilderMetadataManager;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DataBuilderMetadataManagerOptionalTest {
    @Test
    public void testRegister() throws Exception {
        val dataBuilderMetadataManager = new DataBuilderMetadataManager()
                .registerWithOptionals(ImmutableSet.of("A", "B"),
                                       ImmutableSet.of("G"),
                                       "C",
                                       "BuilderA",
                                       TestBuilderA.class);
        try {
            dataBuilderMetadataManager.registerWithOptionals(ImmutableSet.of("A", "B"),
                                                             ImmutableSet.of("G"),
                                                             "C",
                                                             "BuilderA",
                                                             TestBuilderB.class);
        }
        catch (DataBuilderFrameworkException e) {
            assertEquals(DataBuilderFrameworkException.ErrorCode.BUILDER_EXISTS, e.getErrorCode());
            return;
        }
        fail("Duplicate error should have come");
    }
}