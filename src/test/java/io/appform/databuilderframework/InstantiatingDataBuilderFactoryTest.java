package io.appform.databuilderframework;

import com.google.common.collect.ImmutableSet;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.engine.impl.InstantiatingDataBuilderFactory;
import io.appform.databuilderframework.model.Data;
import io.appform.databuilderframework.model.DataBuilderMeta;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class InstantiatingDataBuilderFactoryTest {
    public static class WrongBuilder extends DataBuilder {

        public WrongBuilder(String blah) {

        }

        @Override
        public Data process(DataBuilderContext context) {
            return null;
        }
    }

    private final DataBuilderMetadataManager dataBuilderMetadataManager = new DataBuilderMetadataManager()
            .register(ImmutableSet.of("A", "B"), "C", "BuilderA", TestBuilderA.class)
            .register(ImmutableSet.of("A", "B"), "C", "BuilderB", null)
            .register(ImmutableSet.of("A", "B"), "X", "BuilderC", WrongBuilder.class);

    private final DataBuilderFactory dataBuilderFactory = new InstantiatingDataBuilderFactory(dataBuilderMetadataManager);

    @Test
    public void testCreate() throws Exception {
        try {
            assertNotNull(dataBuilderFactory.create(
                    DataBuilderMeta.builder()
                            .name("BuilderA")
                            .consumes(Collections.emptySet())
                            .build()));
            dataBuilderFactory.create(DataBuilderMeta.builder()
                                              .name("BuilderB")
                                              .consumes(Collections.emptySet())
                                              .build()); //Should throw
        }
        catch (DataBuilderFrameworkException e) {
            assertEquals(DataBuilderFrameworkException.ErrorCode.NO_BUILDER_FOUND_FOR_NAME, e.getErrorCode());
            return;
        }
        fail();
    }

    @Test
    public void testFail() throws Exception {
        try {
            dataBuilderFactory.create(DataBuilderMeta.builder()
                                              .name("BuilderC")
                                              .consumes(Collections.emptySet())
                                              .build()); //Should throw
        }
        catch (DataBuilderFrameworkException e) {
            assertEquals(DataBuilderFrameworkException.ErrorCode.INSTANTIATION_FAILURE, e.getErrorCode());
            return;
        }
        fail();
    }
}
