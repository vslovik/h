import org.junit.jupiter.api.Test;
import org.unipi.matrixpnorm.Utils;

import static org.junit.jupiter.api.Assertions.*;

class MatrixpnormUtilsTest {

    @Test
    void serializeArrayOfDoublesTest() {

        Double[] row = {9.0, 6.4};

        String serialized = Utils.serializeArrayOfDoubles(row);
        String expected = "9\t6.4";

        assertEquals(expected, serialized);
    }

    @Test
    void deserializeTest() {

        Double[] deserialized = Utils.deserializeArrayOfDoubles("9.6586\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0");

        Double[] expected = {9.6586, 6.0, 0.0, 1.0, 1.0, 0.0, 3.0, 6.0};

        assertArrayEquals(expected, deserialized);
    }
}
