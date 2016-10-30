package com.unity.challenge.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TopNRecordsMapperTest {

  MapDriver<LongWritable, Text, DoubleWritable, Text> mapDriver;

  @BeforeClass
  public void setup() {
    TopNRecordsMapper mapper = new TopNRecordsMapper();
    mapDriver = MapDriver.newMapDriver(mapper);
  }

  @Test
  public void mapperTest() throws IOException, InterruptedException {

    Text t1 = new Text("ATL-PHX,4");
    Text t2 = new Text("SFO-BOS,10");
    mapDriver.withInput(new LongWritable(1l), t1).withInput(new LongWritable(2l), t2)
        .withOutput(new DoubleWritable(4.0), t1).withOutput(new DoubleWritable(10.0), t2).runTest();

  }
}
