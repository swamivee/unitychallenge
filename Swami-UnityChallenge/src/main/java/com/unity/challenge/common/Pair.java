package com.unity.challenge.common;

/**
 * Util class used to store source and destination
 * @author sveerama
 *
 * @param <F>
 * @param <S>
 */
public class Pair<F, S> {
  private F first; //first member of pair
  private S second; //second member of pair

  public Pair(F first, S second) {
    this.first = first;
    this.second = second;
  }

  public void setFirst(F first) {
    this.first = first;
  }

  public void setSecond(S second) {
    this.second = second;
  }

  public F getFirst() {
    return first;
  }

  public S getSecond() {
    return second;
  }
}
