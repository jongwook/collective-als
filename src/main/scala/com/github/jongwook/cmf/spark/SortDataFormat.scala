package com.github.jongwook.cmf.spark

abstract class SortDataFormat[K, Buffer] {

  /**
    * Creates a new mutable key for reuse. This should be implemented if you want to override
    * [[getKey(Buffer, Int, K)]].
    */
  def newKey(): K = null.asInstanceOf[K]

  /** Return the sort key for the element at the given index. */
  protected def getKey(data: Buffer, pos: Int): K

  /**
    * Returns the sort key for the element at the given index and reuse the input key if possible.
    * The default implementation ignores the reuse parameter and invokes [[getKey(Buffer, Int]].
    * If you want to override this method, you must implement [[newKey()]].
    */
  def getKey(data: Buffer, pos: Int, reuse: K): K = {
    getKey(data, pos)
  }

  /** Swap two elements. */
  def swap(data: Buffer, pos0: Int, pos1: Int): Unit

  /** Copy a single element from src(srcPos) to dst(dstPos). */
  def copyElement(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int): Unit

  /**
    * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
    * Overlapping ranges are allowed.
    */
  def copyRange(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int, length: Int): Unit

  /**
    * Allocates a Buffer that can hold up to 'length' elements.
    * All elements of the buffer should be considered invalid until data is explicitly copied in.
    */
  def allocate(length: Int): Buffer
}
