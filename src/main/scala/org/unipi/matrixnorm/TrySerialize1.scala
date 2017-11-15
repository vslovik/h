// https://tech.zilverline.com/2011/04/07/serializing-strings-using-scalacheck

package org.unipi.matrixnorm

object TrySerialize1 extends App {
  def serialize(string: String) = string.getBytes
  def deserialize(bytes: Array[Byte]) = new String(bytes)

  def test(string: String) {
    val actual = deserialize(serialize(string))
    println(if (string == actual) "OK ".format(string)
    else "FAIL expected  got ".format(string, actual))
  }

  test("")
  test("foo")
  test("the quick brown fox jumps over the lazy dog")
  test("\u2192")
}