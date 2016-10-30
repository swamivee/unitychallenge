package com.unity.challenge.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * Util class used to maintain the pair in writable method
 * @author sveeramani
 * 
 */
public class PairWritable implements WritableComparable<PairWritable> {
  String source;
  String destination;

  public PairWritable() {

  }

  public PairWritable(Pair<String, String> p) {
    source = p.getFirst();
    destination = p.getSecond();
  }

  public PairWritable(String s1, String s2) {
    source = s1;
    destination = s2;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, source);
    Text.writeString(out, destination);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    source = Text.readString(in);
    destination = Text.readString(in);
  }

  @Override
  public int compareTo(PairWritable pw2) {
    String src = pw2.getSource();
    String dest = pw2.getDestination();

    if (source.equals(src)) {
      return destination.compareTo(dest);
    }
    return source.compareTo(src);
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getDestination() {
    return destination;
  }

  public void setDestination(String destination) {
    this.destination = destination;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    PairWritable pair = (PairWritable) o;
    return source.equals(pair.getSource()) && destination.equals(pair.getDestination());
  }

  public void set(String t1, String t2) {
    source = t1;
    destination = t2;
  }

  public String toString() {
    return source + "-" + destination;
  }

}
