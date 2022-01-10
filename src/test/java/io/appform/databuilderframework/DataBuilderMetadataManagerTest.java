package io.appform.databuilderframework;

import com.google.common.collect.ImmutableSet;
import io.appform.databuilderframework.engine.DataBuilderFrameworkException;
import io.appform.databuilderframework.engine.DataBuilderMetadataManager;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DataBuilderMetadataManagerTest {
    @Test
    public void testRegister() throws Exception {
        val dataBuilderMetadataManager = new DataBuilderMetadataManager()
                .register(ImmutableSet.of("A", "B"), "C", "BuilderA", TestBuilderA.class);
        try {
            dataBuilderMetadataManager.register(ImmutableSet.of("A", "B"), "C", "BuilderA", TestBuilderB.class);
        }
        catch (DataBuilderFrameworkException e) {
            assertEquals(DataBuilderFrameworkException.ErrorCode.BUILDER_EXISTS, e.getErrorCode());
            return;
        }
        fail("Duplicate error should have come");
    }
}
