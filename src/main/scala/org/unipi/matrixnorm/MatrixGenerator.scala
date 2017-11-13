package org.unipi.matrixnorm

import scala.reflect._
import org.scalacheck._

class MatrixGenerator {

//  def matrix[T: ClassTag](g: Gen[T])(order: Int): Gen[Array[Array[T]]] =
//    Gen.listOfN(order*order, g).map { squareList =>
//      squareList.toArray.grouped(order).toArray
//    }
//
//  val r = new scala.util.Random
//  val order:Int = r.nextInt(5)
//  val limit:Double = r.nextInt(100)
//
//  matrix(Gen.choose(0.0, 2.0 + limit))(2 + order).sample.foreach{
//    rows => rows.foreach{
//      cols => cols.foreach{
//        i => print(s"\t$i")
//      }
//        println("\n")
//    }
//  }

  def matrix[T: ClassTag](g: Gen[T])(rows: Int, cols: Int): Gen[Array[Array[T]]] =
    Gen.listOfN(rows*cols, g).map { squareList =>
      squareList.toArray.grouped(cols).toArray
    }

//  val r = new scala.util.Random
//  val rows:Int = r.nextInt(50)
//  val cols:Int = r.nextInt(50)
//  val limit:Double = r.nextInt(100)
//
//  matrix(Gen.choose(0.0, 2.0 + limit))(2 + rows, 2 + cols)

//  matrix(Gen.choose(0.0, 2.0 + limit))(2 + rows, 2 + cols).sample.foreach{
//    rows => rows.foreach{
//      cols => cols.foreach{
//        i => print(s"\t$i")
//      }
//        println("\n")
//    }
//  }

//  def matrix[ T : ClassTag]( g: Gen[ T ] )( order: Int ): Gen[ Array[ Array[ T ] ] ] =
//    for {
//      rowSeq <- Gen.listOfN( order, g )
//      rowArray = rowSeq.toArray
//      seqOfRowArrays <- Gen.listOfN( order, rowArray)
//    } yield seqOfRowArrays.toArray
//
//  matrix(Gen.choose(0.0,10.0))(30).sample.foreach{
//    rows => rows.foreach{
//      cols => cols.foreach{
//        i => print(s"\t$i")
//      }
//        println("\n")
//    }
//  }

}
