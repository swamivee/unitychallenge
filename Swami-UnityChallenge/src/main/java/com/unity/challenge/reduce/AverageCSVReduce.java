package com.unity.challenge.reduce;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.unity.challenge.common.MetricPair;
import com.unity.challenge.common.PairMetric;
import com.unity.challenge.common.PairWritable;


/**
 * Reducer which comes after @AverageCSVMapper
 * This stores the top X records based on the averageDelayTime in a priorityqueue
 * and then writes it out in the {@link #cleanup(org.apache.hadoop.mapreduce.Reducer.Context)}
 * 
 * @author sveerama
 *
 */
public class AverageCSVReduce extends Reducer<PairWritable, MetricPair, Text, NullWritable> {
  public static enum ReducerCounters {
    TOTAL_INPUT_RECORDS,
    TOTAL_REDUCER_RECORDS,
    TOTAL_REMOVED_RECORDS,
    TOTAL_OUTPUT_RECORDS
  };
  int noOfTopRecords = 0;
  long count = 0l;
  double timeDelay = 0d;
  double averageTimeDelay = 0d;
  PriorityQueue<PairMetric> queue = new PriorityQueue<PairMetric>();
  private static final Log LOG = LogFactory.getLog(AverageCSVReduce.class);

  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    noOfTopRecords = conf.getInt("TOP_RECORDS", 100);
  }

  @Override
  public void reduce(PairWritable key, Iterable<MetricPair> values, Context context)
      throws IOException, InterruptedException {

    for (MetricPair m : values) {
      count += m.getCount();
      timeDelay += m.getSum();
      context.getCounter(ReducerCounters.TOTAL_INPUT_RECORDS).increment(1);
    }
    //Write the record into PQ and then removes if it exceeds the count
    if (count > 0) {
      averageTimeDelay = (double) timeDelay / count;
      PairMetric pm = new PairMetric(key, averageTimeDelay);
      queue.offer(pm);
      if (queue.size() > noOfTopRecords) {
        context.getCounter(ReducerCounters.TOTAL_REMOVED_RECORDS).increment(1);
        queue.poll();
      }
    }

    count = 0l;
    timeDelay = 0d;
    averageTimeDelay = 0d;
  }

  @Override
  public void cleanup(Context context) throws IOException {
    PairMetric metric = null;
    Text output = new Text();
    try {
      while (!queue.isEmpty()) {
        context.getCounter(ReducerCounters.TOTAL_OUTPUT_RECORDS).increment(1);
        metric = queue.poll();
        output.set(metric.toString());
        context.write(output, NullWritable.get());
      }
      super.cleanup(context);
    } catch (InterruptedException e) {
      LOG.error(e);
    }
  }
}
