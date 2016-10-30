package com.unity.challenge.common;

import junit.framework.Assert;

import org.testng.annotations.Test;


/**
 * 
 * @author sveeramani
 * 
 */

public class PairWritableTest {

  @Test
  public void compareTest() {
    PairWritable pw1 = new PairWritable("SFO", "BOS");
    PairWritable pw2 = new PairWritable("SFO", "BSS");
    Assert.assertTrue(pw1.compareTo(pw2) != 0);
    Assert.assertTrue(pw2.getSource().equals("SFO"));
    Assert.assertTrue(pw1.getDestination().equals("BOS"));
  }

  @Test
  public void toStringTest() {
    PairWritable pw1 = new PairWritable("SFO", "BOS");
    Assert.assertTrue(pw1.toString().equals("SFO-BOS"));
  }

  @Test
  public void equalToTest() {
    PairWritable pw1 = new PairWritable("SFO", "BOS");
    PairWritable pw2 = new PairWritable("SFO", "BOS");
    Assert.assertTrue(pw1.equals(pw2));
  }
}
