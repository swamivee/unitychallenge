package com.unity.challenge.reduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * The data input to the reducer comes sorted based on the avereage delay time in the descending order
 * hence we output the reducer after Top X records are written out
 * 
 * @author sveerama
 *
 */

public class TopNRecordsReducer extends Reducer<DoubleWritable, Text, Text, NullWritable> {
  public static enum ReducerCounters {
    TOTAL_INPUT_RECORDS,
    TOTAL_REDUCER_RECORDS,
    TOTAL_REMOVED_RECORDS,
    TOTAL_OUTPUT_RECORDS
  };
  int noOfTopRecords = 0;

  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    noOfTopRecords = conf.getInt("TOP_RECORDS", 100);
  }

  @Override
  public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    for (Text m : values) {
      context.getCounter(ReducerCounters.TOTAL_INPUT_RECORDS).increment(1);

      if (noOfTopRecords > 0) {
        context.getCounter(ReducerCounters.TOTAL_OUTPUT_RECORDS).increment(1);
        context.write(m, NullWritable.get());
        noOfTopRecords--;
      } else {
        break;
      }
    }
  }
}
