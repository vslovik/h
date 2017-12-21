import org.unipi.matrixnorm.Utils;
import org.unipi.matrixgen.MatrixGenRecordReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class MatrixGenRecordReaderTest {

    @Test
    public void wrongInput() throws IOException, InterruptedException {
        MatrixGenRecordReader reader = new MatrixGenRecordReader();

        double progress = 0.0;
        int i = 0;
        while (reader.nextKeyValue()) {
            assertEquals(reader.getCurrentKey().getClass(), Text.class);
            assertEquals(reader.getCurrentValue().getClass(), NullWritable.get().getClass());
            assertTrue(reader.getProgress() > progress);
            progress = reader.getProgress();
            Double[][] matrix = Utils.deserialize(reader.getCurrentKey().toString());
            assertTrue(matrix.length > 0);
            assertTrue(matrix[0].length > 0);
            i += 1;
        }

        assertTrue(100 == i);
    }
}
