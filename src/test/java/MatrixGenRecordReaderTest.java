import org.unipi.matrixnorm.Utils;
import org.unipi.matrixgen.MatrixGenRecordReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

import org.junit.Test;
import org.junit.Assert;

public class MatrixGenRecordReaderTest {

    @Test
    public void wrongInput() throws IOException, InterruptedException {
        MatrixGenRecordReader reader = new MatrixGenRecordReader();

        double progress = 0.0;
        int i = 0;
        while (reader.nextKeyValue()) {
            Assert.assertEquals(reader.getCurrentKey().getClass(), Text.class);
            Assert.assertEquals(reader.getCurrentValue().getClass(), NullWritable.get().getClass());
            Assert.assertTrue(reader.getProgress() > progress);
            progress = reader.getProgress();
            Double[][] matrix = Utils.deserialize(reader.getCurrentKey().toString());
            Assert.assertTrue(matrix.length > 0);
            Assert.assertTrue(matrix[0].length > 0);
            i += 1;
        }

        Assert.assertTrue(100 == i);
    }
}
