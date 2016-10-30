package com.unity.challenge.common;

import org.testng.Assert;
import org.testng.annotations.Test;


public class MetricPairTest {
  @Test
  public void initTest() {
    MetricPair mp = new MetricPair(10);
    Assert.assertEquals(mp.getCount(), 1);
    Assert.assertEquals(mp.getSum(), 10.0);
  }

  @Test
  public void toStringTest() {
    MetricPair mp = new MetricPair(10);
    Assert.assertEquals(mp.toString(), "1,10.0");
  }

  @Test
  public void compareTest() {
    MetricPair mp1 = new MetricPair(10);
    MetricPair mp2 = new MetricPair(20);
    Assert.assertTrue(!mp1.equals(mp2));
  }
}
