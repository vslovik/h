// https://alvinalexander.com/scala/how-to-write-text-files-in-scala-printwriter-filewriter

package org.unipi.matrixnorm

import java.io._

object TryWrite extends App {

  val text: String = "bla bla bla"

  val pw = new PrintWriter(new File("data/a" ))
  pw.write("Hello, world")
  pw.close()

  val Statement = StringBuilder.newBuilder


  val currentDirectory = new java.io.File(".").getCanonicalPath

  Statement.append(currentDirectory).append("/data/b")

  println(Statement.toString)

  val file = new File(Statement.toString)
  val bw = new BufferedWriter(new FileWriter(file))
  bw.write(text)
  bw.close()

}