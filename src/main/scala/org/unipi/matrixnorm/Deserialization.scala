package org.unipi.matrixnorm

import scala.io.Source

object Deserialization extends App {

  val filename = "data/matrix"
  for (line <- Source.fromFile(filename).getLines) {
    val items = line.split("\t")

    val rows = items.drop(1)
    println(rows)
    val cols = items.drop(1)
    println(cols)

    items.foreach(println)
  }
}
