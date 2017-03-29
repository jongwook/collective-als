package com.github.jongwook.cmf.spark

import java.util.Comparator

class Sorter[K, Buffer](private val s: SortDataFormat[K, Buffer]) {

  private val timSort = new TimSort(s)

  /**
   * Sorts the input buffer within range [lo, hi).
   */
  def sort(a: Buffer, lo: Int, hi: Int, c: Comparator[_ >: K]): Unit = {
    timSort.sort(a, lo, hi, c)
  }
}
