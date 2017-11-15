package org.unipi.matrixnorm

import org.scalacheck._
import scala.reflect._

object TryMatrixGen1 extends App {

  def matrix[T: ClassTag](g: Gen[T])(order: Int): Gen[Array[Array[T]]] =
    for {
      rowSeq <- Gen.listOfN(order, g)
      rowArray = rowSeq.toArray
      seqOfRowArrays <- Gen.listOfN(order, rowArray)
    } yield seqOfRowArrays.toArray

  matrix(Gen.choose(0.0, 10.0))(30).sample.foreach {
    rows =>
      rows.foreach {
        cols =>
          cols.foreach {
            i => print(s"\t$i")
          }
          println("\n")
      }
  }
}