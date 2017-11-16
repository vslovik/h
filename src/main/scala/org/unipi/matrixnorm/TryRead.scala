// https://alvinalexander.com/scala/how-to-open-read-text-files-in-scala-cookbook-examples
// ToDo read whole chapter

package org.unipi.matrixnorm

import scala.io.Source

object TryRead extends App {

  val filename = "data/matrix"
  for (line <- Source.fromFile(filename).getLines) {
    println(line)
  }
}