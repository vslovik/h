// https://stackoverflow.com/questions/28785064/scala-convert-arraystring-to-arraydouble
// https://github.com/functional-streams-for-scala/fs2
//http://blog.dmitryleskov.com/programming/scala/stream-hygiene-iii-scalaz-ephemeralstream-fills-quite-a-canyon/
//https://en.wikibooks.org/wiki/Scala/Traits
//https://stackoverflow.com/questions/6545066/using-scala-from-java-passing-functions-as-parameters
//http://www.codecommit.com/blog/java/interop-between-java-and-scala

//https://stackoverflow.com/questions/6545066/using-scala-from-java-passing-functions-as-parameters
//https://github.com/scala/scala-java8-compat
//http://javatoscala.com/
//http://lampwww.epfl.ch/~michelou/scala/using-scala-from-java.html
// https://egkatzioura.com/2017/02/16/wordcount-on-hadoop-with-scala/
// https://hadoopi.wordpress.com/2013/05/27/understand-recordreader-inputsplit/ ***
// https://stackoverflow.com/questions/1888716/what-replaces-class-variables-in-scala ***

//ToDo 1. Write generated data to hadoop
//ToDo 2.
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
