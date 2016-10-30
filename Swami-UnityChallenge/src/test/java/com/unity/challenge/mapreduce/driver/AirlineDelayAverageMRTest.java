package com.unity.challenge.mapreduce.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.unity.challenge.common.MetricPair;
import com.unity.challenge.common.Pair;
import com.unity.challenge.common.PairWritable;
import com.unity.challenge.mapper.AverageCSVMapper;
import com.unity.challenge.reduce.AverageCSVCombiner;
import com.unity.challenge.reduce.AverageCSVReduce;


public class AirlineDelayAverageMRTest {

  MapDriver<LongWritable, Text, PairWritable, MetricPair> mapDriver;
  ReduceDriver<PairWritable, MetricPair, Text, NullWritable> reducerDriver;
  MapReduceDriver<LongWritable, Text, PairWritable, MetricPair, Text, NullWritable> mapReduceDriver;
  MapReduceDriver<LongWritable, Text, PairWritable, MetricPair, Text, NullWritable> mapReduceDriver2;

  @BeforeClass
  public void setup() {
    AverageCSVMapper mapper = new AverageCSVMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
    AverageCSVReduce reducer = new AverageCSVReduce();
    reducerDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    mapReduceDriver.setCombiner(new AverageCSVCombiner());
    mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void mapperTest() throws IOException, InterruptedException {
    Text t1 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,-6,-2,ATL,PHX,1587,45,13,0,,0,0,0,0,0,0");
    Text t2 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,6,-2,SFO,BOS,1587,45,13,0,,0,0,0,0,0,0");
    Text t3 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,NA,-2,SFO,BOS,1587,45,13,0,,0,0,0,0,0,0");
    Text t4 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,-2,-2,ATL,PHX,1587,45,13,0,,0,0,0,0,0,0");
    mapDriver.withInput(new LongWritable(1l), t1).withInput(new LongWritable(2l), t2)
        .withInput(new LongWritable(3l), t3).withInput(new LongWritable(4l), t4)
        .withOutput(new PairWritable(new Pair<String, String>("ATL", "PHX")), new MetricPair(6))
        .withOutput(new PairWritable(new Pair<String, String>("ATL", "PHX")), new MetricPair(2))
        .runTest();
    Assert.assertEquals(
        mapDriver.getCounters().findCounter(AverageCSVMapper.MapperCounters.NO_DELAY_RECORDS)
            .getValue(), 1);
    Assert.assertEquals(
        mapDriver.getCounters().findCounter(AverageCSVMapper.MapperCounters.FILTERED_RECORD)
            .getValue(), 1);
    Assert.assertEquals(
        mapDriver.getCounters().findCounter(AverageCSVMapper.MapperCounters.TOTAL_INPUT_RECORDS)
            .getValue(), 4);
    Assert.assertEquals(
        mapDriver.getCounters().findCounter(AverageCSVMapper.MapperCounters.TOTAL_OUTPUT_RECORDS)
            .getValue(), 2);

  }

  @Test
  public void reducerTest() throws IOException, InterruptedException {
    PairWritable wr1 = new PairWritable(new Pair<String, String>("ATL", "PHX"));
    MetricPair mp1 = new MetricPair(6);
    MetricPair mp2 = new MetricPair(3);

    PairWritable wr2 = new PairWritable(new Pair<String, String>("SFO", "BOS"));

    List<MetricPair> values = new ArrayList<MetricPair>();
    values.add(mp1);
    values.add(mp2);
    MetricPair mp3 = new MetricPair(10);

    List<MetricPair> values1 = new ArrayList<MetricPair>();
    values1.add(mp3);
    reducerDriver.withInput(wr1, values).withOutput(new Text("ATL-PHX,4.5"), NullWritable.get())
        .withInput(wr2, values1).withOutput(new Text("SFO-BOS,10"), NullWritable.get()).runTest();
    Assert.assertEquals(
        reducerDriver.getCounters()
            .findCounter(AverageCSVReduce.ReducerCounters.TOTAL_INPUT_RECORDS).getValue(), 3);
    Assert.assertEquals(
        reducerDriver.getCounters()
            .findCounter(AverageCSVReduce.ReducerCounters.TOTAL_OUTPUT_RECORDS).getValue(), 2);

  }

  @Test
  public void testMapReduce() throws IOException {
    Text inputT1 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,-6,-2,ATL,PHX,1587,45,13,0,,0,0,0,0,0,0");
    Text inputT2 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,-2,-2,ATL,PHX,1587,45,13,0,,0,0,0,0,0,0");
    Text inputT3 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,-10,-2,SFO,BOS,1587,45,13,0,,0,0,0,0,0,0");

    Text outputT1 = new Text("ATL-PHX,4");
    Text outputT2 = new Text("SFO-BOS,10");
    mapReduceDriver.withInput(new LongWritable(), inputT1).withInput(new LongWritable(), inputT2)
        .withInput(new LongWritable(), inputT3).withOutput(outputT1, NullWritable.get())
        .withOutput(outputT2, NullWritable.get()).runTest();

  }

  @Test
  public void testMapReduceTestTopRecords() throws IOException {
    Text inputT1 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,-6,-2,ATL,PHX,1587,45,13,0,,0,0,0,0,0,0");
    Text inputT2 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,-2,-2,ATL,PHX,1587,45,13,0,,0,0,0,0,0,0");
    Text inputT3 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,-10,-2,SFO,BOS,1587,45,13,0,,0,0,0,0,0,0");

    Text outputT2 = new Text("SFO-BOS,10");
    mapReduceDriver2.getConfiguration().setInt("TOP_RECORDS", 1);

    mapReduceDriver2.withInput(new LongWritable(), inputT1).withInput(new LongWritable(), inputT2)
        .withInput(new LongWritable(), inputT3).withOutput(outputT2, NullWritable.get()).runTest();

  }
}
