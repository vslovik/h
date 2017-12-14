package org.unipi.matrixgen

import org.scalacheck.Gen

import scala.math.BigDecimal

class MatrixGenerator {

  def decimal(d: Double, precision: Int = 4): Double = {
    BigDecimal(d).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def matrix[Double](g: Gen[Double])(rows: Int, cols: Int): Gen[List[List[Double]]] =
    Gen.listOfN(rows * cols, g).map { squareList =>
      squareList.grouped(cols).toList
    }

  def serializeList(l: List[List[Double]]):String = {
    l.flatten.map{
      j: Double => decimal(j)
    }.mkString(Array(l.size, l.head.size).mkString("", "\t", "\t"), "\t", "")
  }

  def serialize(matrix: Any): String = {
    matrix match {
      case Some(i) =>  i match {
        case y: List[List[Double]] => serializeList(y)
      }
      case x: List[List[Double]] => serializeList(x)
      case _ => "Unknown list value type"
    }
  }

  @throws[Exception]
  def deserialize(s: String): List[List[Double]] = {
    val items = s.split("\t")
    val n = items.head.toInt * items(1).toInt
    if(n != items.length - 2)
      throw new IllegalArgumentException
    items.drop(2).toList.map{
      y => y.toDouble
    }.grouped(Integer.parseInt(items(1))).toList
  }

}