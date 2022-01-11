package io.appform.databuilderframework.engine;

import com.google.common.collect.ImmutableSet;
import io.appform.databuilderframework.complextest.SB;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DataBuilderMetadataManagerTest {

    @Test
    public void testGetConsumesSetFor() throws Exception {
        val manager = new DataBuilderMetadataManager();
        assertNull(manager.getConsumesSetFor("A"));
        manager.register(ImmutableSet.of("CR", "CAID", "VAS"), "OO", "SB", SB.class);
        assertEquals(1, manager.getConsumesSetFor("CR").size());
    }
}
