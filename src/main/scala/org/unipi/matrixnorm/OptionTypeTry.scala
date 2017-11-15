// https://alvinalexander.com/scala/using-scala-option-some-none-idiom-function-java-null

/** A powerful Scala idiom is to use the Option class when returning a value from a function that can be null.
Simply stated, instead of returning one object when a function succeeds and null when it fails, your function
should instead return an instance of an Option, where the Option object is either:
*/

/** An instance of the Scala Some class An instance of the Scala None class Because Some and None are both
children of Option, your function signature just declares that you're returning an Option that contains some
type (such as the Int type shown below). At the very least, this has the tremendous benefit of letting the
user of your function know what’s going on.
*/

package org.unipi.matrixnorm

object OptionTypeTry extends App {

  def toInt(in: String): Option[Int] = {
    try {
      Some(Integer.parseInt(in.trim))
    } catch {
      case e: NumberFormatException => None
    }
  }

  toInt("someString") match {
    case Some(i) => println(i)
    case None => println("That didn't work.")
  }

  toInt("1") match {
    case Some(i) => println(i)
    case None => println("That didn't work.")
  }
}