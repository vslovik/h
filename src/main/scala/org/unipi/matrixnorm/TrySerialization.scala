package org.unipi.matrixnorm

import java.io._

object TrySerialization extends App {

  def serialise(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

  def deserialise(bytes: Array[Byte]): Any = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    value
  }

  println(deserialise(serialise("My Test")))
  println(deserialise(serialise(List(1))))
  println(deserialise(serialise(Map(1 -> 2))))

}