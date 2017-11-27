package org.unipi.matrixnorm

object Utils {

  def compare(thisSeq: Seq[Int], thatSeq: Seq[Int]): Int = {
    if(0 == thisSeq.head.compareTo(thatSeq.head)) {
      0 //compare(thisSeq.drop(0), thatSeq.drop(0)) // ToDo
    } else {
      thisSeq.head.compareTo(thatSeq.head)
    }
  }

}
