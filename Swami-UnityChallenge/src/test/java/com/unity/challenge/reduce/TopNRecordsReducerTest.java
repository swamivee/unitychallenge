package com.unity.challenge.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.unity.challenge.reduce.TopNRecordsReducer;


public class TopNRecordsReducerTest {

  MapDriver<LongWritable, Text, DoubleWritable, Text> mapDriver;
  ReduceDriver<DoubleWritable, Text, Text, NullWritable> reducerDriver;
  MapReduceDriver<LongWritable, Text, DoubleWritable, Text, Text, NullWritable> mapReduceDriver;
  MapReduceDriver<LongWritable, Text, DoubleWritable, Text, Text, NullWritable> mapReduceDriver2;

  @BeforeClass
  public void setup() {
    TopNRecordsReducer reducer = new TopNRecordsReducer();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);
  }

  @Test
  public void reducerTest() throws IOException, InterruptedException {
    Text t1 = new Text("ATL-PHX,11.0");
    Text t2 = new Text("SFO-BOS,10.0");
    List<Text> lst1 = new ArrayList<Text>();
    lst1.add(t1);
    List<Text> lst2 = new ArrayList<Text>();
    lst2.add(t2);

    reducerDriver.withInput(new DoubleWritable(11.0), lst1)
        .withInput(new DoubleWritable(10.0), lst2).withOutput(t1, NullWritable.get())
        .withOutput(t2, NullWritable.get()).runTest();

    Assert.assertEquals(
        reducerDriver.getCounters()
            .findCounter(TopNRecordsReducer.ReducerCounters.TOTAL_INPUT_RECORDS).getValue(), 2);
    Assert.assertEquals(
        reducerDriver.getCounters()
            .findCounter(TopNRecordsReducer.ReducerCounters.TOTAL_OUTPUT_RECORDS).getValue(), 2);

  }
}
