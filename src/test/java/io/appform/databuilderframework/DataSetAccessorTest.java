package io.appform.databuilderframework;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.appform.databuilderframework.model.DataDelta;
import io.appform.databuilderframework.model.DataSet;
import lombok.val;
import org.junit.Test;

import static org.junit.Assert.*;

public class DataSetAccessorTest {

    @Test
    public void testGet() throws Exception {
        val dataSet = new DataSet();
        val dataSetAccessor = DataSet.accessor(dataSet);
        dataSetAccessor.merge(new TestDataA("RandomValue"));
        val testDataA = dataSetAccessor.get("A", TestDataA.class);
        assertEquals("RandomValue", testDataA.getValue());
        assertNull(dataSetAccessor.get("X", TestDataA.class));

        try {
            dataSetAccessor.get("A", TestDataB.class);
        } catch (ClassCastException e) {
            return;
        }
        fail();
    }

    @Test
    public void testMerge() throws Exception {
        val dataSet = new DataSet();
        val dataSetAccessor = DataSet.accessor(dataSet);
        val dataDelta = new DataDelta(Lists.newArrayList(new TestDataA("Hello"),
                                                                new TestDataB("World")));
        dataSetAccessor.merge(dataDelta);
        val testDataA = dataSetAccessor.get("A", TestDataA.class);
        assertEquals("Hello", testDataA.getValue());
        val testDataB = dataSetAccessor.get("B", TestDataB.class);
        assertEquals("World", testDataB.getValue());
    }

    @Test
    public void testCheckForData() throws Exception {
        val dataSet = new DataSet();
        val dataSetAccessor = DataSet.accessor(dataSet);
        val dataDelta = new DataDelta(Lists.newArrayList(new TestDataA("Hello"),
                new TestDataB("World")));
        dataSetAccessor.merge(dataDelta);
        assertTrue(dataSetAccessor.checkForData("A"));
        assertFalse(dataSetAccessor.checkForData("X"));
        assertTrue(dataSetAccessor.checkForData(ImmutableSet.of("A", "B")));
        assertFalse(dataSetAccessor.checkForData(ImmutableSet.of("A", "X")));
        assertFalse(dataSetAccessor.checkForData(ImmutableSet.of("X", "A")));
        assertFalse(dataSetAccessor.checkForData(ImmutableSet.of("X", "Ys")));
    }
}
