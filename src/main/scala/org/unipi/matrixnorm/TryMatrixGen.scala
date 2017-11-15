package org.unipi.matrixnorm

import scala.reflect._
import org.scalacheck._

object TryMatrixGen extends App {

  def matrix[T: ClassTag](g: Gen[T])(order: Int): Gen[Array[Array[T]]] =
    Gen.listOfN(order * order, g).map { squareList =>
      squareList.toArray.grouped(order).toArray
    }

  val r = new scala.util.Random
  val order: Int = r.nextInt(5)
  val limit: Double = r.nextInt(100)

  matrix(Gen.choose(0.0, 2.0 + limit))(2 + order).sample.foreach {
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