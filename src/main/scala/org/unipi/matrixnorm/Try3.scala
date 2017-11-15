/*
ScalaCheck generates random test examples
and uses these to test programmer specified propositions.
This sounds more complicated than it is in practice.

Here we reuse the same string serialization code from the
previous example, but instead of manually thinking of examples
to test we use ScalaCheck’s Prop.forAll method to create
a proposition.

You can basically read lines 7 to 9 as: “For all strings s,
s must be equal to the result of serializing and deserializing s“.
On the last line we tell ScalaCheck to check this proposition.
ScalaCheck does so by generating random strings and passing
these to our proposition
*/

package org.unipi.matrixnorm
import org.scalacheck._

object Try3 extends App {

  def serialize(string: String) = string.getBytes("UTF-8")
  def deserialize(bytes: Array[Byte]) = new String(bytes, "UTF-8")

  val prop_serializes_all_strings = Prop.forAll { s: String =>
    s == deserialize(serialize(s))
  }

  prop_serializes_all_strings.check
}