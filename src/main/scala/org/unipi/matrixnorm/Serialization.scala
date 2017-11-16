package org.unipi.matrixnorm

import org.scalacheck._
import java.io._

object Serialization extends App {
  val mg = new MatrixGenerator
  val r = new scala.util.Random
  val pw = new PrintWriter(new File("data/matrix"))
  for (a <- 1 until 10) {
    val rows: Int = r.nextInt(5)
    val cols: Int = r.nextInt(5)
    val limit: Double = r.nextInt(100)
    pw.println(mg.serialize(mg.matrix(Gen.choose(0.0, 2.0 + limit))(2 + rows, 2 + cols).sample))
  }
  pw.close()
}
