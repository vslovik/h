import org.unipi.matrixnorm.Utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class MatrixnormUtilsTest {

    @Test
    void serializeTest() {

        Double[][] matrix = {
                {9.0, 6.0},
                {0.0, 1.0},
                {1.0, 0.0},
                {3.0, 6.0}
        };

        String serialized = Utils.serialize(matrix);
        String expected = "4\t2\t9.0\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0";

        assertEquals(expected, serialized);
    }

    @Test
    void deserializeTest() {

        Double[][] deserialized = Utils.deserialize("4\t2\t9.0\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0");

        Double[][] expected = {
                {9.0, 6.0},
                {0.0, 1.0},
                {1.0, 0.0},
                {3.0, 6.0}
        };

        assertArrayEquals(expected, deserialized);
    }

    @Test
    void wrongInputTest() {

        assertThrows(IllegalArgumentException.class,
                ()->{
                    Utils.deserialize("1\t2\t1.0");
                });

    }
}
