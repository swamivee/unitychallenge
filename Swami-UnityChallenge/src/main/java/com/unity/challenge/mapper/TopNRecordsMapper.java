package com.unity.challenge.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Read the output of the previous MR output and split the text into double (arrival delay)
 * and the reset of the record
 * 
 * K=arrival_delay
 * V=Entire record
 * @author sveerama
 *
 */
public class TopNRecordsMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

  public static enum MapperCounters {
    EMPTY_LINE,
    TOTAL_INPUT_RECORDS,
    TOTAL_OUTPUT_RECORDS
  };
  private final DoubleWritable outputKey = new DoubleWritable();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
    context.getCounter(MapperCounters.TOTAL_INPUT_RECORDS).increment(1);
    if (value != null) {
      double d = Double.parseDouble(value.toString().split(",")[1]);
      outputKey.set(d);
      context.write(outputKey, value);
      context.getCounter(MapperCounters.TOTAL_OUTPUT_RECORDS).increment(1);

    } else {
      context.getCounter(MapperCounters.EMPTY_LINE).increment(1);
    }
  }

}
