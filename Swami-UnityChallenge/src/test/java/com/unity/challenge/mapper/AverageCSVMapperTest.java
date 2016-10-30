package com.unity.challenge.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.unity.challenge.common.MetricPair;
import com.unity.challenge.common.Pair;
import com.unity.challenge.common.PairWritable;


public class AverageCSVMapperTest {

  MapDriver<LongWritable, Text, PairWritable, MetricPair> mapDriver;

  @BeforeClass
  public void setup() {
    AverageCSVMapper mapper = new AverageCSVMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
  }

  @Test
  public void mapperTest() throws IOException, InterruptedException {
    Text t1 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,6,-2,ATL,PHX,1587,45,13,0,,0,0,0,0,0,0");
    Text t2 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,-6,-2,SFO,BOS,1587,45,13,0,,0,0,0,0,0,0");
    Text t3 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,NA,-2,SFO,BOS,1587,45,13,0,,0,0,0,0,0,0");
    Text t4 =
        new Text(
            "2006,1,11,3,743,745,1024,1018,US,343,N657AW,281,273,223,2,-2,ATL,PHX,1587,45,13,0,,0,0,0,0,0,0");
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

}
