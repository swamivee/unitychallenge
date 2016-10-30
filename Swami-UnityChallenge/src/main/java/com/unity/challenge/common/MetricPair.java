package com.unity.challenge.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * Used to store the sum and count which will be used in the reducer to compute average
 * 
 * @author sveerama
 *
 */
public class MetricPair implements Writable {
  long count;
  double sum;

  public MetricPair() {

  }

  public MetricPair(double sum) {
    this.count = 1l;
    this.sum = sum;
  }

  public MetricPair(long count, double sum) {
    this.count = count;
    this.sum = sum;
  }

  public void set(long count, double sum) {
    this.count = count;
    this.sum = sum;
  }

  public void set(double sum) {
    this.sum = sum;
    this.count = 1l;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public long getCount() {
    return count;
  }

  public void setSum(double sum) {
    this.sum = sum;
  }

  public double getSum() {
    return sum;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(count);
    out.writeDouble(sum);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    count = in.readLong();
    sum = in.readDouble();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  public String toString() {
    return Long.toString(count) + "," + Double.toString(sum);
  }

  @Override
  public boolean equals(Object o) {
    MetricPair pair = (MetricPair) o;
    return count == pair.getCount() && sum == pair.getSum();
  }
}
