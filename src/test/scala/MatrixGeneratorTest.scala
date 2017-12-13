import org.junit.Test
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import org.unipi.matrixgen.MatrixGenerator

class MatrixGeneratorTest extends FlatSpec with MockitoSugar {

  @Test
  def serializeTest(): Unit = {
    val generator = new MatrixGenerator

    val matrix = Array(
      Array(9.0, 6.0).toList,
      Array(0.0, 1.0).toList,
      Array(1.0, 0.0).toList,
      Array(3.0, 6.0).toList
    ).toList

    val serialized = generator.serialize(matrix)
    val expected = "4\t2\t9.0\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0"

    assert(expected == serialized, true)
  }

  @Test
  def deserializeTest(): Unit = {
    val generator = new MatrixGenerator

    val deserialized = generator.deserialize("4\t2\t9.0\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0")

    val expected = Array(
      Array(9.0, 6.0).toList,
      Array(0.0, 1.0).toList,
      Array(1.0, 0.0).toList,
      Array(3.0, 6.0).toList
    ).toList

    assert(expected == deserialized, true)
  }

//  @Test
//  def wrongInputTest(): Unit = {
//
//    (new MatrixGenerator).deserialize("1\t2\t1.0")
//    intercept()
//
//  }

}