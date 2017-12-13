import org.junit.Test
import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import org.unipi.matrixnorm.{HadoopMatrixNorm, MapperKey, MapperValue}

class HadoopMatrixNormPartitionerTest extends FlatSpec with MockitoSugar {

  @Test
  def columnIndexPartitioningTest(): Unit = {
    val partitioner = new HadoopMatrixNorm.ColumnIndexPartitioner
    val partition1 = partitioner.getPartition(
      new MapperKey(0, 0, 0),
      new MapperValue(0, 0, 9.0),
      2
    )
    val partition2 = partitioner.getPartition(
      new MapperKey(0, 0, 1),
      new MapperValue(0, 0, 9.0),
      2
    )

    assert(partition1 == partition2, true)
  }

}