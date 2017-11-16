// http://danielwestheide.com/blog/2012/12/12/the-neophytes-guide-to-scala-part-4-pattern-matching-anonymous-functions.html
package org.unipi.matrixnorm

import org.scalacheck._

import scala.math.BigDecimal

class MatrixGenerator {

  def decimal(d: Double, precision: Int = 4): Double = {
    BigDecimal(d).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def matrix[Double](g: Gen[Double])(rows: Int, cols: Int): Gen[List[List[Double]]] =
    Gen.listOfN(rows * cols, g).map { squareList =>
      squareList.grouped(cols).toList
    }

  def matrixSerialized[Double](g: Gen[Double])(rows: Int, cols: Int): Gen[String] =
    Gen.listOfN(rows * cols, g).map { squareList =>
      squareList.map {
        item => item match {
          case i: scala.Double => decimal(i)
        }
      }.mkString(Array(rows, cols).mkString("", "\t", "\t"), "\t", "")
    }

  def serialize(matrix: Any): String = {
    matrix match {
      case Some(i) =>  i match {
        case y: List[List[Double]] => y.flatten.map{
          j: Double => decimal(j)
        }.mkString(Array(y.size, y.head.size).mkString("", "\t", "\t"), "\t", "")
      }
    }
  }

  def deserialize(s: String): List[List[Double]] =
  {
    val items = s.split("\t")
    // val rows = Integer.parseInt(items(0)) ToDo assert: check rows
    items.drop(2).toList.map{
      y => y.toDouble
    }.grouped(Integer.parseInt(items(1))).toList
  }

}
