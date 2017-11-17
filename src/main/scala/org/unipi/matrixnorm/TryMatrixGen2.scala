package org.unipi.matrixnorm

import org.scalacheck._
import scala.math.BigDecimal

object TryMatrixGen2 extends App {

  def decimal(d: Double, precision: Int = 4): Double = {
    BigDecimal(d).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def matrixSerialized[Double](g: Gen[Double])(rows: Int, cols: Int): Gen[String] =
    Gen.listOfN(rows * cols, g).map { squareList =>
      squareList.map {
        case (i: scala.Double) => decimal(i)
      }.mkString(Array(rows, cols).mkString("", "\t", "\t"), "\t", "")
    }

  matrixSerialized(Gen.choose(0.0, 10.0))(3, 5).sample.foreach(println)
}