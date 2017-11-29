package org.unipi.matrixnorm

import org.unipi.matrixgen.MatrixGenerator
import scala.collection.JavaConverters._

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

  def mapToArray(): Unit = {
    val mg = new MatrixGenerator
    var matrix = new java.util.TreeMap[Int, Array[Double]]
    var colMap = new java.util.TreeMap[Int, Double]
    for(i <- Seq(0, 1, 2, 3))
      colMap.put(i,  4)
    matrix.put(0, colMap.values().asScala.toArray)

    val rows = matrix.ceilingEntry(matrix.ceilingKey(0)).getValue.length
    val cols = matrix.size()
    val data = Array.ofDim[Double](rows, cols)

    var c = 0
    for(col <- matrix.values().asScala.toArray) {
      var r = 0
      for(colValue <- col) {
        data(r)(c) = colValue
        r += 1
      }
      c += 1
    }

    println(data)
    //println(mg.serialize(data))
  }

  mapToArray()
}
