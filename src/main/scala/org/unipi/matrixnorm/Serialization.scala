package org.unipi.matrixnorm

import org.scalacheck._
import java.io._

object Serialization extends App {

  val mg = new MatrixGenerator
  val r = new scala.util.Random
  val pw = new PrintWriter(new File("data/matrix"))

  for (a <- 1 until 100) {
    val rows: Int = r.nextInt(50)
    val cols: Int = r.nextInt(50)
    val limit: Double = r.nextInt(100)
    mg.matrixSerialized(Gen.choose(0.0, 2.0 + limit))(2 + rows, 2 + cols).sample match {
      case Some(i) =>
        println(i)
        pw.println(i)
      case None => println("That didn't work.")
    }
  }

  pw.close()
}
