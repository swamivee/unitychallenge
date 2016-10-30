package com.unity.challenge.reduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import com.unity.challenge.common.MetricPair;
import com.unity.challenge.common.PairWritable;


/**
 * This is the combiner used in the @AirlineDelayAverageMR
 * 
 * This is used to intermediately aggregate the mapper output from @AverageCSVMapper
 * It is the aggregate of count and sum
 * 
 * @author sveerama
 *
 */
public class AverageCSVCombiner extends Reducer<PairWritable, MetricPair, PairWritable, MetricPair> {
  static enum ReducerCounters {
    TOTAL_INPUT_RECORDS,
    TOTAL_COMBINER_RECORDS
  };
  long count = 0l;
  double timeDelay = 0d;

  MetricPair mp = new MetricPair();

  @Override
  public void reduce(PairWritable key, Iterable<MetricPair> values, Context context)
      throws IOException, InterruptedException {

    for (MetricPair m : values) {
      count += m.getCount();
      timeDelay += m.getSum();
      context.getCounter(ReducerCounters.TOTAL_INPUT_RECORDS).increment(1);
    }
    context.getCounter(ReducerCounters.TOTAL_COMBINER_RECORDS).increment(1);
    mp.set(count, timeDelay);
    context.write(key, mp);
    count = 0l;
    timeDelay = 0d;
  }
}
