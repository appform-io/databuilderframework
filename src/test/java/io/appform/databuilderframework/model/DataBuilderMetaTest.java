package io.appform.databuilderframework.model;

import com.google.common.collect.ImmutableSet;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DataBuilderMetaTest {
    @Test
    public void testEquals() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        val rhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        assertEquals(lhs, rhs);
        assertEquals(lhs.hashCode(), rhs.hashCode());
    }
    
    @Test
    public void testEqualsWithOptionalsAndAccess() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test",ImmutableSet.of("O"),ImmutableSet.of("G"));
        val rhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test",ImmutableSet.of("O"),ImmutableSet.of("G"));
        assertEquals(lhs, rhs);
        assertEquals(lhs.hashCode(), rhs.hashCode());
    }


    
    @Test
    public void testNotEquals1() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        val rhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test1");
        assertNotEquals(lhs, rhs);
    }
    
    @Test
    public void testNotEquals2() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        val rhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "D", "test");
        assertNotEquals(lhs, rhs);
    }

    @Test
    public void testNotEquals3() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        val rhs = new DataBuilderMeta(ImmutableSet.of("A", "X"), "C", "test");
        assertNotEquals(lhs, rhs);
    }

    @Test
    public void testNotEquals4() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        val rhs = new DataBuilderMeta(ImmutableSet.of("A"), "D", "test");
        assertNotEquals(lhs, rhs);
    }

    @Test
    public void testNotEquals5() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        val rhs = new DataBuilderMeta(ImmutableSet.of("A", "X"), "D", "test");
        assertNotEquals(lhs, rhs);
        assertNotEquals(lhs.hashCode(), rhs.hashCode());
    }

    @Test
    public void testNotEquals6() {
        DataBuilderMeta lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        Assert.assertTrue(lhs.equals(lhs));
    }

    @Test
    public void testNotEquals7() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        assertNotEquals(null, lhs);
    }

    @Test
    public void testNotEquals8() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        assertNotEquals(lhs, new Integer(100));
    }
    
    @Test
    public void testNotEqualsWithOptionalsAndAccess1() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test",ImmutableSet.of("O","H"),ImmutableSet.of("G"));
        val rhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test",ImmutableSet.of("O"),ImmutableSet.of("G"));
        assertNotEquals(lhs, rhs);
    }
    
    @Test
    public void testNotEqualsWithOptionalsAndAccess2() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test",ImmutableSet.of("O"),ImmutableSet.of("G"));
        val rhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test",ImmutableSet.of("O"),ImmutableSet.of("H"));
        assertNotEquals(lhs, rhs);
    }

    @Test
    public void testNotEqualsWithOptionalsAndAccess3() {
        val lhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test",ImmutableSet.of("O"),ImmutableSet.of("G"));
        val rhs = new DataBuilderMeta(ImmutableSet.of("A", "B"), "C", "test");
        assertNotEquals(lhs, rhs);
    }
}
