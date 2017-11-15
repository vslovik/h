/* ScalaCheck allows us to label a test case. Labeling our proposition with the
 integral values of the string contents should provide us with more information:
*/

package org.unipi.matrixnorm

import org.scalacheck._, Prop._

object Try4 extends App {

  def serialize(string: String) = string.getBytes("UTF-8")
  def deserialize(bytes: Array[Byte]) = new String(bytes, "UTF-8")

  def decode(string: String) = "characters = " + string.map(_.toInt).mkString(",")

  val prop_serializes_all_strings = Prop.forAll { s: String =>
    decode(s) |: s == deserialize(serialize(s))
  }

  prop_serializes_all_strings.check

}