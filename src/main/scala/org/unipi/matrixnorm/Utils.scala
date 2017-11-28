package org.unipi.matrixnorm

object Utils extends App {

  @throws[IllegalArgumentException]
  def compare(thisSeq: Seq[Int], thatSeq: Seq[Int]): Int = {
    if(thisSeq.size != thatSeq.size)
      throw new IllegalArgumentException("Arguments have to be of the same size")
    if(0 == thisSeq.head.compareTo(thatSeq.head)) {
      if(thisSeq.size > 1) compare(thisSeq.drop(1), thatSeq.drop(1))
      else 0
    } else {
      thisSeq.head.compareTo(thatSeq.head)
    }
  }
}
