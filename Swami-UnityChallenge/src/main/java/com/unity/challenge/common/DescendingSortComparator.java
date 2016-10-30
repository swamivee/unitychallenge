package com.unity.challenge.common;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;


/**
 * Sort comparator to take the 2 average values and sort descending for reducer 
 * This is used for sending the @TopNRecordsMapper output into @TopNRecordsReducer
 * The data is sorted in descending order for the reducer to just write out Top X records
 * 
 * @author sveerama
 *
 */
public class DescendingSortComparator extends WritableComparator {
  private static final Logger logger = Logger.getLogger(DescendingSortComparator.class);

  public DescendingSortComparator() {
    super(DoubleWritable.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    DoubleWritable k1 = (DoubleWritable) w1;
    DoubleWritable k2 = (DoubleWritable) w2;
    int cmp = k1.compareTo(k2);
    return -1 * cmp;
  }
}
