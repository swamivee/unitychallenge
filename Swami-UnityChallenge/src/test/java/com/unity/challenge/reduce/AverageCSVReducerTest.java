package com.unity.challenge.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.unity.challenge.common.MetricPair;
import com.unity.challenge.common.Pair;
import com.unity.challenge.common.PairWritable;


public class AverageCSVReducerTest {

  ReduceDriver<PairWritable, MetricPair, Text, NullWritable> reducerDriver;

  @BeforeClass
  public void setup() {
    AverageCSVReduce reducer = new AverageCSVReduce();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);
  }

  @Test
  public void reducerTest() throws IOException, InterruptedException {
    PairWritable wr1 = new PairWritable(new Pair<String, String>("ATL", "PHX"));
    MetricPair mp1 = new MetricPair(6);
    MetricPair mp2 = new MetricPair(2);

    PairWritable wr2 = new PairWritable(new Pair<String, String>("SFO", "BOS"));

    List<MetricPair> values = new ArrayList<MetricPair>();
    values.add(mp1);
    values.add(mp2);
    MetricPair mp3 = new MetricPair(10);

    List<MetricPair> values1 = new ArrayList<MetricPair>();
    values1.add(mp3);
    reducerDriver.withInput(wr1, values)
        .withInput(wr2, values1)
        .withOutput(new Text("SFO-BOS,10"), NullWritable.get())
        .withOutput(new Text("ATL-PHX,4"), NullWritable.get())
        .runTest();
    Assert.assertEquals(
        reducerDriver.getCounters()
            .findCounter(AverageCSVReduce.ReducerCounters.TOTAL_INPUT_RECORDS).getValue(), 3);
    Assert.assertEquals(
        reducerDriver.getCounters()
            .findCounter(AverageCSVReduce.ReducerCounters.TOTAL_OUTPUT_RECORDS).getValue(), 2);

  }
}
