package com.unity.challenge.common;

import java.text.DecimalFormat;


/**
 * Util class used to create the source and destination pair and the average time
 * 
 * This is used in reducer to filter out top X values based on the average time and hence implements Comparable
 * 
 * @author sveerama
 *
 */
public class PairMetric implements Comparable<PairMetric> {

  String pair;
  double averageTime;

  public PairMetric() {
  }

  public PairMetric(PairWritable pair, double averageTime) {
    this.pair = pair.toString();
    this.averageTime = averageTime;
  }

  public PairMetric(String key, double averageTime) {
    this.pair = key;
    this.averageTime = averageTime;
  }

  public String getPair() {
    return pair;
  }

  public double getAverageTime() {
    return averageTime;
  }

  public void setAverageTime(double averageTime) {
    this.averageTime = averageTime;
  }

  @Override
  public int compareTo(PairMetric o) {
    return Double.valueOf(this.averageTime).compareTo(o.averageTime);
  }

  public String toString() {
    DecimalFormat f = new DecimalFormat("#####.####");
    return pair.toString() + "," + f.format(this.averageTime);
  }

}
