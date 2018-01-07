import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.unipi.matrixrowgen.MatrixRowGenRecordReader;
import org.unipi.matrixpnorm.Utils;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatrixRowGenRecordReaderTest {

    @Test
    void genReaderTest() throws IOException, InterruptedException {
        MatrixRowGenRecordReader reader = new MatrixRowGenRecordReader();

        double progress = 0.0;
        int i = 0;
        while (reader.nextKeyValue()) {
            assertEquals(reader.getCurrentKey().getClass(), Integer.class);
            assertEquals(reader.getCurrentValue().getClass(), Text.class);
            assertTrue(reader.getProgress() > progress);
            progress = reader.getProgress();
            Double[] row = Utils.deserializeArrayOfDoubles(reader.getCurrentKey().toString());
            assertTrue(row.length > 0);
            i += 1;
        }

        assertTrue(100 == i);
    }
}