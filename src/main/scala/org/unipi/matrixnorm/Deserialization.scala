// https://stackoverflow.com/questions/28785064/scala-convert-arraystring-to-arraydouble
package org.unipi.matrixnorm

import scala.io.Source

object Deserialization extends App {

  val filename = "data/matrix"
  for (line <- Source.fromFile(filename).getLines) {
    val items = line.split("\t")

    val numbers = items.map(x => x.toDouble)

    numbers.foreach(println)
  }
}
