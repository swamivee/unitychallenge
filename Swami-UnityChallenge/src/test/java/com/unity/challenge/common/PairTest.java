package com.unity.challenge.common;

import junit.framework.Assert;

import org.testng.annotations.Test;


/**
 * 
 * @author sveeramani
 * 
 */

public class PairTest {

  @Test
  public void compareTest() {
    Pair<String, String> p1 = new Pair<String, String>("SFO", "BOS");
    Assert.assertTrue(p1.getFirst().equals("SFO"));
    Assert.assertTrue(p1.getSecond().equals("BOS"));
  }

}
