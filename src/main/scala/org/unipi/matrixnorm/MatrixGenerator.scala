package org.unipi.matrixnorm

import scala.reflect._
import org.scalacheck._
import scala.math.BigDecimal

class MatrixGenerator {

  def matrix[T: ClassTag](g: Gen[T])(rows: Int, cols: Int): Gen[Array[Array[T]]] =
    Gen.listOfN(rows * cols, g).map { squareList =>
      squareList.toArray.grouped(cols).toArray
    }

  def decimal(d: Double, precision: Int = 4): Double = {
    BigDecimal(d).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def matrixSerialized[T: ClassTag](g: Gen[T])(rows: Int, cols: Int): Gen[String] =
    Gen.listOfN(rows * cols, g).map { squareList =>
      squareList.map { item =>
        item match {
          case i: Int => decimal(i.toDouble)
          case l: Long => decimal(l.toDouble)
          case d: Double => decimal(d)
          case _ => None
        }
      }.mkString(Array(rows, cols).mkString("", "\t", "\t"), "\t", "") //ToDo delimiter val
    }
}
