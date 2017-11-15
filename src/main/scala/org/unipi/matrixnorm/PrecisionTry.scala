// https://stackoverflow.com/questions/8385458/how-to-compare-floating-point-values-in-scala

package org.unipi.matrixnorm

import scala.math.BigDecimal

object PrecisionTry extends App {

  val number:Double = BigDecimal(1.23456789).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

  println(number)

  println("%.4f".format(0.714999999999))

}