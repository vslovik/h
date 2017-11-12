package org.unipi.matrixnorm

import scala.reflect._
import org.scalacheck._

//object MatrixGenerator {
//
//  def matrix[T](g: Gen[T])(order: Int): Gen[Array[Seq[T]]] =
//    Gen.listOfN(order, Gen.listOfN(order, g)).map(_.toArray)
//
//  def main(args: Array[String]): Unit =
//    matrix(Gen.choose(1,10))(3)
//    println('a')
//
//}

object MatrixGenerator extends App {

  def matrix[T: ClassTag](g: Gen[T])(order: Int): Gen[Array[Array[T]]] =
    Gen.listOfN(order*order, g).map { squareList =>
      squareList.toArray.grouped(order).toArray
    }

  matrix(Gen.choose(1,10))(3)
  println('a')

}
