import org.unipi.matrixnorm.HadoopMatrixNorm;
import org.unipi.matrixnorm.MapperKey;
import org.unipi.matrixnorm.MapperValue;

import org.junit.Test;
import org.junit.Assert;

public class HadoopMatrixNormPartitionerTest {

    @Test
    public void columnIndexPartitioningTest() {
        HadoopMatrixNorm.ColumnIndexPartitioner partitioner = new HadoopMatrixNorm.ColumnIndexPartitioner();

        int partition1 = partitioner.getPartition(
                new MapperKey(0, 0, 0),
                new MapperValue(0, 0, 9.0),
                2
        );
        int partition2 = partitioner.getPartition(
                new MapperKey(0, 0, 1),
                new MapperValue(0, 0, 9.0),
                2
        );

        Assert.assertEquals(partition1, partition2);
    }
}
