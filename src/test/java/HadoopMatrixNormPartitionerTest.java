import org.unipi.matrixnorm.HadoopMatrixNorm;
import org.unipi.matrixnorm.MapperKey;
import org.unipi.matrixnorm.MapperValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class HadoopMatrixNormPartitionerTest {

    @Test
    void columnIndexPartitioningTest() {
        HadoopMatrixNorm.ColumnIndexPartitioner partitioner = new HadoopMatrixNorm.ColumnIndexPartitioner();

        MapperValue value = new MapperValue(0, 0, 9.0);

        int partition1 = partitioner.getPartition(
                new MapperKey(0, 0, 0),
                value,
                2
        );
        int partition2 = partitioner.getPartition(
                new MapperKey(0, 0, 1),
                value,
                2
        );

        assertEquals(partition1, partition2);
    }
}
