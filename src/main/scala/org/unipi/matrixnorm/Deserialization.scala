// https://stackoverflow.com/questions/28785064/scala-convert-arraystring-to-arraydouble
package org.unipi.matrixnorm

import scala.io.Source

object Deserialization extends App {

  val filename = "data/matrix"
  for (line <- Source.fromFile(filename).getLines) {

    val matrix = (new MatrixGenerator).deserialize(line)

    matrix.foreach {
      row => row.foreach{
        col => print(s"\t$col")
      }
        print("\n")
    }
  }
}
