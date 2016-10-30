package com.unity.challenge.common;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DescendingSortComparatorTest {

  @Test
  public void sortComparatorTest() throws IOException, InterruptedException {
    DoubleWritable k1 = new DoubleWritable(20.0);
    DoubleWritable k2 = new DoubleWritable(30.0);
    DescendingSortComparator sortComprator = new DescendingSortComparator();
    Assert.assertTrue(sortComprator.compare(k1, k2) > 0);
  }
}
