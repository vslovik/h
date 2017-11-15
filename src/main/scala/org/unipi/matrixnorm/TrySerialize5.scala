/* In our case, we’re only interested in serializing valid Unicode strings.
The Java String class does not provide a method for this, so we have to use
a CharsetEncoder instead. Unfortunately, this requires digging into some
of the lower-level APIs provided by Java NIO.

The serialization code has been changed to directly use the Charset encoder
and decoder and the proposition is changed to include a guard condition
using the ScalaCheck ==> operator: the proposition only holds for strings
that can be encoded.
*/

package org.unipi.matrixnorm

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.Charset
import org.scalacheck._, Prop._

object TrySerialize5 extends App {

  def encoder = Charset.forName("UTF-8").newEncoder
  def decoder = Charset.forName("UTF-8").newDecoder

  def serialize(string: String) = {
    val bytes = encoder.encode(CharBuffer.wrap(string))
    bytes.array.slice(bytes.position, bytes.limit)
  }
  def deserialize(bytes: Array[Byte]) = decoder.decode(ByteBuffer.wrap(bytes)).toString
  def decode(string: String) = "characters = " + string.map(_.toInt).mkString(",")

  val prop_serializes_all_strings = Prop.forAll { s: String =>
    encoder.canEncode(s) ==> (decode(s) |: s == deserialize(serialize(s)))
  }

  prop_serializes_all_strings.check



}