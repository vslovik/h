import org.unipi.matrixnorm.{HadoopMatrixNorm_, MapperKey, MapperValue}
import org.apache.hadoop.io._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import org.junit.Test

class HadoopMatrixNormMapperTest extends FlatSpec with MockitoSugar {

  @Test
  def wrongInput(): Unit = {
    val mapper = new HadoopMatrixNorm_.MatrixNormMapper
    val context = mock[mapper.Context]

    intercept[java.lang.IllegalArgumentException] {
      mapper.map(
        key = null,
        value = new Text("1\t2\t1.0"),
        context
      )
    }

    verify(context, never()).write(new MapperKey(0, 0, 0), new MapperValue(0, 0, 1.0))

  }

  @Test
  def oneElementMatrixTest(): Unit = {
    val mapper = new HadoopMatrixNorm_.MatrixNormMapper
    val context = mock[mapper.Context]

    mapper.map(
      key = null,
      value = new Text("1\t1\t1.0"),
      context
    )

    verify(context).write(new MapperKey(0, 0, 0), new MapperValue(0, 0, 1.0))
    verify(context).write(new MapperKey(0, 0, 1), new MapperValue(0, 0, 1.0))

  }

  @Test
  def zeroMatrixTest(): Unit = {
    val mapper = new HadoopMatrixNorm_.MatrixNormMapper
    val context = mock[mapper.Context]

    mapper.map(
      key = null,
      value = new Text("2\t2\t0.0\t0.0\t0.0\t0.0"),
      context
    )

    verify(context).write(new MapperKey(0, 0, 0), new MapperValue(0, 0, 0.0))
    verify(context).write(new MapperKey(0, 0, 1), new MapperValue(0, 0, 0.0))
    verify(context).write(new MapperKey(0, 1, 0), new MapperValue(0, 0, 0.0))
    verify(context).write(new MapperKey(0, 1, 1), new MapperValue(0, 0, 0.0))

    verify(context).write(new MapperKey(0, 0, 0), new MapperValue(0, 1, 0.0))
    verify(context).write(new MapperKey(0, 0, 1), new MapperValue(0, 1, 0.0))
    verify(context).write(new MapperKey(0, 1, 0), new MapperValue(0, 1, 0.0))
    verify(context).write(new MapperKey(0, 1, 1), new MapperValue(0, 1, 0.0))

  }

  @Test
  def nonZeroMatrixTest(): Unit = {
    val mapper = new HadoopMatrixNorm_.MatrixNormMapper
    val context = mock[mapper.Context]

    mapper.map(
      key = null,
      value = new Text("4\t2\t9.0\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0"),
      context
    )

    verify(context).write(new MapperKey(0, 0, 0), new MapperValue(0, 0, 9.0))
    verify(context).write(new MapperKey(0, 0, 1), new MapperValue(0, 0, 9.0))
    verify(context).write(new MapperKey(0, 1, 0), new MapperValue(0, 0, 6.0))
    verify(context).write(new MapperKey(0, 1, 1), new MapperValue(0, 0, 6.0))

    verify(context).write(new MapperKey(0, 0, 0), new MapperValue(0, 1, 0.0))
    verify(context).write(new MapperKey(0, 0, 1), new MapperValue(0, 1, 0.0))
    verify(context).write(new MapperKey(0, 1, 0), new MapperValue(0, 1, 1.0))
    verify(context).write(new MapperKey(0, 1, 1), new MapperValue(0, 1, 1.0))

    verify(context).write(new MapperKey(0, 0, 0), new MapperValue(0, 2, 1.0))
    verify(context).write(new MapperKey(0, 0, 1), new MapperValue(0, 2, 1.0))
    verify(context).write(new MapperKey(0, 1, 0), new MapperValue(0, 2, 0.0))
    verify(context).write(new MapperKey(0, 1, 1), new MapperValue(0, 2, 0.0))

    verify(context).write(new MapperKey(0, 0, 0), new MapperValue(0, 3, 3.0))
    verify(context).write(new MapperKey(0, 0, 1), new MapperValue(0, 3, 3.0))
    verify(context).write(new MapperKey(0, 1, 0), new MapperValue(0, 3, 6.0))
    verify(context).write(new MapperKey(0, 1, 1), new MapperValue(0, 3, 6.0))

  }

}