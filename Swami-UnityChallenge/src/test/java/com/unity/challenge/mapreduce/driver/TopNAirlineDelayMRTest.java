package com.unity.challenge.mapreduce.driver;

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

import com.unity.challenge.common.DescendingSortComparator;
import com.unity.challenge.mapper.TopNRecordsMapper;
import com.unity.challenge.reduce.TopNRecordsReducer;


public class TopNAirlineDelayMRTest {

  MapDriver<LongWritable, Text, DoubleWritable, Text> mapDriver;
  ReduceDriver<DoubleWritable, Text, Text, NullWritable> reducerDriver;
  MapReduceDriver<LongWritable, Text, DoubleWritable, Text, Text, NullWritable> mapReduceDriver;
  MapReduceDriver<LongWritable, Text, DoubleWritable, Text, Text, NullWritable> mapReduceDriver2;

  @BeforeClass
  public void setup() {
    TopNRecordsMapper mapper = new TopNRecordsMapper();
    TopNRecordsReducer reducer = new TopNRecordsReducer();
    DescendingSortComparator sortComprator = new DescendingSortComparator();
    mapDriver = MapDriver.newMapDriver(mapper);
    reducerDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    mapReduceDriver.setKeyOrderComparator(sortComprator);
    mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    mapReduceDriver2.setKeyOrderComparator(sortComprator);
  }

  @Test
  public void mapperTest() throws IOException, InterruptedException {

    Text t1 = new Text("ATL-PHX,4.0");
    Text t2 = new Text("SFO-BOS,10.0");
    mapDriver.withInput(new LongWritable(1l), t1).withInput(new LongWritable(2l), t2)
        .withOutput(new DoubleWritable(4.0), t1).withOutput(new DoubleWritable(10.0), t2).runTest();

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

  @Test
  public void testMapReduce() throws IOException {
    Text t1 = new Text("ATL-PHX,9.0");
    Text t2 = new Text("SFO-BOS,20.0");

    mapReduceDriver.withInput(new LongWritable(), t1).withInput(new LongWritable(), t2)
        .withOutput(t2, NullWritable.get()).withOutput(t1, NullWritable.get()).runTest();

  }

  @Test
  public void testMapReduceTestTopRecords() throws IOException {
    Text inputT1 = new Text("ATL-PHX,12.0");
    Text inputT2 = new Text("SFO-BOS,20.0");
    mapReduceDriver2.getConfiguration().setInt("TOP_RECORDS", 1);

    mapReduceDriver2.withInput(new LongWritable(), inputT1).withInput(new LongWritable(), inputT2)
        .withOutput(inputT2, NullWritable.get()).runTest();

  }
}
