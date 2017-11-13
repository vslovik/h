package org.unipi.matrixnorm

import org.scalacheck._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

object Serialization extends App {

  def serialise(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray
  }

  def deserialise(bytes: Array[Byte]): Any = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close
    value
  }

    val r = new scala.util.Random
    val rows:Int = r.nextInt(50)
    val cols:Int = r.nextInt(50)
    val limit:Double = r.nextInt(100)

  val s = (new MatrixGenerator).matrix(Gen.choose(0.0, 2.0 + limit))(2 + rows, 2 + cols).sample

  s.foreach{
    rows => rows.foreach{
      cols => cols.foreach{
        i => print(s"\t$i")
      }
        println("\n")
    }
  }

  var ss = deserialise(serialise(s))

//  ss.foreach{
//    rows => rows.foreach{
//      cols => cols.foreach{
//        i => print(s"\t$i")
//      }
//        println("\n")
//    }
//  }

//  (new MatrixGenerator).matrix(Gen.choose(0.0, 2.0 + limit))(2 + rows, 2 + cols).sample.foreach{
//      rows => rows.foreach{
//        cols => cols.foreach{
//          i => print(s"\t$i")
//        }
//          println("\n")
//      }
//    }


  println(deserialise(serialise("My Test")))
  println(deserialise(serialise(List(1))))
  println(deserialise(serialise(Map(1 -> 2))))
  println(deserialise(serialise(1)))
}
