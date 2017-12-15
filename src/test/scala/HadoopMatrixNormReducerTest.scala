import org.apache.hadoop.io._
import org.junit.Test
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import org.unipi.matrixnorm.{HadoopMatrixNorm, MapperKey, MapperValue}
import scala.collection.JavaConverters._

class HadoopMatrixNormReducerTest extends FlatSpec with MockitoSugar {

  @Test
  def oneElementMatrixTest(): Unit = {
    val reducer = new HadoopMatrixNorm.MatrixNormReducer
    val context = mock[reducer.Context]

    reducer.reduce(key = new MapperKey(0, 0, 0),
      values = Seq(new MapperValue(0, 0, 0.0)).asJava,
      context
    )

    reducer.reduce(key = new MapperKey(0, 0, 1),
      values = Seq(new MapperValue(0, 0, 0.0)).asJava,
      context
    )

    verify(context, never()).write(NullWritable.get(), "1\t1\t0.0")

    reducer.cleanup(context)

    verify(context).write(NullWritable.get(), "1\t1\t0.0")

  }

  @Test
  def emitZeroMatrixTest(): Unit = {
    val reducer = new HadoopMatrixNorm.MatrixNormReducer
    val context = mock[reducer.Context]

    reducer.reduce(key = new MapperKey(0, 0, 0),
      values = Seq(new MapperValue(0, 0, 0.0),new MapperValue(0, 1, 0.0),new MapperValue(0, 2, 0.0),new MapperValue(0, 3, 0.0)).asJava,
      context
    )
    reducer.reduce(key = new MapperKey(0, 0, 1),
      values = Seq(new MapperValue(0, 0, 0.0),new MapperValue(0, 1, 0.0),new MapperValue(0, 2, 0.0),new MapperValue(0, 3, 0.0)).asJava,
      context
    )
    reducer.reduce(key = new MapperKey(0, 1, 0),
      values = Seq(new MapperValue(0, 0, 0.0),new MapperValue(0, 1, 0.0),new MapperValue(0, 2, 0.0),new MapperValue(0, 3, 0.0)).asJava,
      context
    )
    reducer.reduce(key = new MapperKey(0, 1, 1),
      values = Seq(new MapperValue(0, 0, 0.0),new MapperValue(0, 1, 0.0),new MapperValue(0, 2, 0.0),new MapperValue(0, 3, 0.0)).asJava,
      context
    )

    verify(context, never()).write(NullWritable.get(), "4\t2\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0")

    reducer.cleanup(context)

    verify(context).write(NullWritable.get(), "4\t2\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0")

  }

  @Test
  def emitMatrixTest(): Unit = {
    val reducer = new HadoopMatrixNorm.MatrixNormReducer
    val context = mock[reducer.Context]

    reducer.reduce(key = new MapperKey(0, 0, 0),
      values = Seq(new MapperValue(0, 0, 9.0),new MapperValue(0, 1, 0.0),new MapperValue(0, 2, 1.0),new MapperValue(0, 3, 3.0)).asJava,
      context
    )
    reducer.reduce(key = new MapperKey(0, 0, 1),
      values = Seq(new MapperValue(0, 0, 9.0),new MapperValue(0, 1, 0.0),new MapperValue(0, 2, 1.0),new MapperValue(0, 3, 3.0)).asJava,
      context
    )
    reducer.reduce(key = new MapperKey(0, 1, 0),
      values = Seq(new MapperValue(0, 0, 6.0),new MapperValue(0, 1, 1.0),new MapperValue(0, 2, 0.0),new MapperValue(0, 3, 6.0)).asJava,
      context
    )
    reducer.reduce(key = new MapperKey(0, 1, 1),
      values = Seq(new MapperValue(0, 0, 6.0),new MapperValue(0, 1, 1.0),new MapperValue(0, 2, 0.0),new MapperValue(0, 3, 6.0)).asJava,
      context
    )

    verify(context, never()).write(NullWritable.get(), "4\t2\t1.0\t1.0\t0.0\t0.1667\t0.1111\t0.0\t0.3333\t1.0")

    reducer.cleanup(context)

    verify(context).write(NullWritable.get(), "4\t2\t1.0\t1.0\t0.0\t0.1667\t0.1111\t0.0\t0.3333\t1.0")

  }

}