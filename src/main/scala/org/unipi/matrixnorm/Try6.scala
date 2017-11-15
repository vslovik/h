/* Another approach is to define a custom generator that only generates valid
 Unicode strings. Generating valid UTF-16 Unicode strings is a little tricky,
  so the code is a bit long:
*/

package org.unipi.matrixnorm

import org.scalacheck._

object Try6 extends App {

  /* we define three character collections that will
  be used to construct correct Unicode data.
   */
  val UnicodeLeadingSurrogate = '\uD800' to '\uDBFF'
  val UnicodeTrailingSurrogate = '\uDC00' to '\uDFFF'
  val UnicodeBasicMultilingualPlane = ('\u0000' to '\uFFFF').diff(UnicodeLeadingSurrogate).diff(UnicodeTrailingSurrogate)

  /* We define a generator for generating a single Unicode character
  from the Basic Multilingual Plane (BMP) (https://en.wikipedia.org/wiki/Plane_(Unicode)).
  It basically picks a random   element from the UnicodeBasicMultilingualPlane collection
  and converts it into a (single character) String.
   */
  val unicodeCharacterBasicMultilingualPlane: Gen[String] = Gen.oneOf(UnicodeBasicMultilingualPlane).map(_.toString)

  /* These 4 lines define a generator for Unicode characters from the supplementary planes.
   It generates a two character string with the first character taken from the leading
   surrogate range and the second character taken from the trailing surrogate range.
   The code uses a for-comprehension since generators are allowed to fail (but in our
   case will never do so).
    */
  val unicodeCharacterSupplementaryPlane: Gen[String] = for {
    c1 <- Gen.oneOf(UnicodeLeadingSurrogate)
    c2 <- Gen.oneOf(UnicodeTrailingSurrogate)
  } yield {
    c1.toString + c2.toString
  }

  /* These lines combine these two generators to produce a new generator that generates a
   BMP character 9 times out of 10, and a supplementary plane character otherwise.
    */
  val unicodeCharacter = Gen.frequency(
    9 -> unicodeCharacterBasicMultilingualPlane,
    1 -> unicodeCharacterSupplementaryPlane)

  /* Finally, this line uses the previous generator to create a list of random but valid
  Unicode characters and concatenates this list into a single string. This is the generator
   we want to use in our proposition.
   */
  val unicodeString = Gen.listOf(unicodeCharacter).map(_.mkString)

  def serialize(string: String) = string.getBytes("UTF-8")
  def deserialize(bytes: Array[Byte]) = new String(bytes, "UTF-8")

  def decode(string: String) = "characters = " + string.map(_.toInt).mkString(",")

  /* This is shown on these lines. Instead of guarding our proposition we now explicitly
   pass in the generator to use in our call to Prop.forAll. Our proposition now passes without
    needing to discard any examples, so we can remove the check parameters.
    */
  val prop_serializes_all_strings = Prop.forAll(unicodeString) { s: String =>
    s == deserialize(serialize(s))
  }

  prop_serializes_all_strings.check

}

/**
With traditional unit tests it is hard to come up with good data that actually exposes bugs,
especially the unexpected kind of bugs that only seem to creep up in production.
Using ScalaCheck we can let the computer come up with random data to test our code.
ScalaCheck includes standard generators for the usual suspects such as Int, String, etc.
It’s also easy to add and use custom generators for existing or new data types.

ScalaCheck is not a replacement for traditional tests. Explicit examples are still useful
as documentation and we should also add a test to the above code to ensure an exception
is thrown when we try to serialize a String that is not valid Unicode, as our proposition
only tests the behavior for valid Strings. But ScalaCheck based testing can help us find
those unexpected bugs (really, the most common kind) before they get into production code.
Trying to find the silent corruption bug of the first few serialization attempts would
be a lot harder in a production environment.
  */