import org.apache.hadoop.io._
import org.junit.Test
import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import org.unipi.matrixgen.{FakeInputSplit, MatrixGenRecordReader, MatrixGenerator}
import org.unipi.matrixnorm.HadoopMatrixNorm_

class MatrixGenRecordReaderTest extends FlatSpec with MockitoSugar {

  @Test
  def wrongInput(): Unit = {
    val reader = new MatrixGenRecordReader
    val mapper = new HadoopMatrixNorm_.MatrixNormMapper
    val context = mock[mapper.Context]
    val mg = new MatrixGenerator

    var progress = 0.0
    var i = 0
    while( reader.nextKeyValue()) {
      assert(reader.getCurrentKey.getClass == (new Text).getClass, true)
      assert(reader.getCurrentValue.getClass == NullWritable.get.getClass, true)
      assert(reader.getProgress > progress, true)
      progress = reader.getProgress
      val matrix = mg.deserialize(reader.getCurrentKey.toString)
      assert(matrix.nonEmpty, true)
      assert(matrix.head.nonEmpty, true)
      i += 1
    }

    assert(100 == i, true)
  }

}