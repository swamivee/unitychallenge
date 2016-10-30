package com.unity.challenge.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import junit.framework.Assert;

import org.testng.annotations.Test;


public class PairMetricTest {
  @Test
  public void sortTest() {

    PairMetric pm1 = new PairMetric(new PairWritable(new Pair<String, String>("a1", "b1")), 5);
    PairMetric pm2 = new PairMetric(new PairWritable(new Pair<String, String>("m", "n")), 2);
    PairMetric pm3 = new PairMetric(new PairWritable(new Pair<String, String>("c", "d")), 3);
    PairMetric pm4 = new PairMetric(new PairWritable(new Pair<String, String>("x", "y")), 1);
    PairMetric pm5 = new PairMetric(new PairWritable(new Pair<String, String>("a", "b")), 4);
    List<PairMetric> lst = new ArrayList<PairMetric>();
    lst.add(pm1);
    lst.add(pm2);
    lst.add(pm3);
    lst.add(pm4);
    lst.add(pm5);
    PriorityQueue<PairMetric> queue = new PriorityQueue<PairMetric>();
    List<PairMetric> list=new ArrayList<PairMetric>();
    for (PairMetric pm : lst) {
      queue.offer(pm);
      if (queue.size() > 2) {
        queue.poll();
      }

    }
    list.addAll(queue);
    Assert.assertTrue(queue.poll().averageTime == 4.0);
    Assert.assertTrue(queue.poll().averageTime == 5.0);
    Assert.assertTrue(queue.isEmpty());
    Assert.assertEquals(list.get(0).averageTime,4.0);
    Assert.assertEquals(list.get(1).averageTime,5.0);
    Collections.sort(list,Collections.reverseOrder());
    Assert.assertEquals(list.get(0).averageTime,5.0);
    Assert.assertEquals(list.get(1).averageTime,4.0);
  }

  @Test
  public void otherTest() {
    PairMetric pm1 = new PairMetric(new PairWritable(new Pair<String, String>("a1", "b1")), 5);
    Assert.assertTrue(pm1.getAverageTime() == 5.0);
    Assert.assertTrue(pm1.getPair().equals("a1-b1"));

  }
}
