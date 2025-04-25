package io.appform.databuilderframework.model;

import io.appform.databuilderframework.engine.Utils;
import junit.framework.TestCase;
import org.junit.Test;

public class DataSetTest extends TestCase {

    public class TestData extends Data {
        protected TestData() {
            super(Utils.name(TestData.class));
        }
    }

    @Test
    public void testKeyRemovalFromDataSetKeyExists() {
        DataSet ds = new DataSet();
        ds.add(new TestData());
        assertNotNull(ds.get(Utils.name(TestData.class)));
        Data removedData = ds.remove(Utils.name(TestData.class));
        assertNotNull(removedData);
        assertNull(ds.get(Utils.name(TestData.class)));
    }

    @Test
    public void testKeyRemovalFromDataSetKeyDoesNotExist() {
        DataSet ds = new DataSet();
        assertNull(ds.get(Utils.name(TestData.class)));
        assertNull(ds.remove(Utils.name(TestData.class)));
        assertNull(ds.get(Utils.name(TestData.class)));
    }

    @Test
    public void testKeyRemovalFromDataSetWithClassAsParameter() {
        DataSet ds = new DataSet();
        ds.add(new TestData());
        assertNotNull(ds.get(Utils.name(TestData.class)));
        Data removedData = ds.remove(TestData.class);
        assertNotNull(removedData);
        assertNull(ds.get(Utils.name(TestData.class)));
    }

}