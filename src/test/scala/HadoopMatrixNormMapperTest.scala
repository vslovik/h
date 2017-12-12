import org.unipi.matrixnorm.{HadoopMatrixNorm, MapperKey, MapperValue}
import org.apache.hadoop.io._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import org.unipi.matrixgen.MatrixGenerator


class HadoopMatrixNormMapperTest extends FlatSpec with MockitoSugar {

  import org.junit.Test
  import java.nio.file.Files
  import java.nio.file.Paths

  @Test // rename src/main/java/main.txt to src/main/java/Main.java
  def renameMainTest(): Unit = {
    val mapper = new HadoopMatrixNorm.MatrixNormMapper
    val context = mock[mapper.Context]

    mapper.map(
      key = null,
      value = new Text("4\t2\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0"),
      context
    )

    val matrix = (new MatrixGenerator).deserialize("4\t2\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0")


    val a = 5
    val b = 2
    assertResult(3) {
      a - b
    }

    assert(matrix.head.head == 0.0, true)
    assert(matrix.length == 4, true)

    val k1 = new MapperKey(0, 0, 0)
    val v1 = new MapperValue(0, 0, matrix.head.head)

    val k2 = new MapperKey(0, 0, 1)
    val v2 = new MapperValue(0, 0, matrix.head.head)


    verify(context, times(16)).write(k1, v1)
    verify(context, times(16)).write(k2, v2)


//    verify(context).write(new MapperKey(0, 1, 0), new MapperValue(0, 0, 6.0))
//    verify(context).write(new MapperKey(0, 1, 1), new MapperValue(0, 0, 6.0))
//
//    verify(context).write(new MapperKey(0, 0, 0), new MapperValue(0, 1, 0.0))
//    verify(context).write(new MapperKey(0, 0, 1), new MapperValue(0, 1, 0.0))
//    verify(context).write(new MapperKey(0, 1, 0), new MapperValue(0, 1, 1.0))
//    verify(context).write(new MapperKey(0, 1, 1), new MapperValue(0, 1, 1.0))
//
//    verify(context).write(new MapperKey(0, 0, 0), new MapperValue(0, 2, 1.0))
//    verify(context).write(new MapperKey(0, 0, 1), new MapperValue(0, 2, 1.0))
//    verify(context).write(new MapperKey(0, 1, 0), new MapperValue(0, 2, 0.0))
//    verify(context).write(new MapperKey(0, 1, 1), new MapperValue(0, 2, 0.0))
//
//    verify(context).write(new MapperKey(0, 0, 0), new MapperValue(0, 3, 3.0))
//    verify(context).write(new MapperKey(0, 0, 1), new MapperValue(0, 3, 3.0))
//    verify(context).write(new MapperKey(0, 1, 0), new MapperValue(0, 3, 6.0))
//    verify(context).write(new MapperKey(0, 1, 1), new MapperValue(0, 3, 6.0))
  }

}