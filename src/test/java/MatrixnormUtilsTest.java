import org.unipi.matrixnorm.Utils;

import org.junit.Test;
import org.junit.Assert;

public class MatrixnormUtilsTest {

    @Test
    public void serializeTest() {

        Double[][] matrix = {
                {9.0, 6.0},
                {0.0, 1.0},
                {1.0, 0.0},
                {3.0, 6.0}
        };

        String serialized = Utils.serialize(matrix);
        String expected = "4\t2\t9.0\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0";

        Assert.assertEquals(expected, serialized);
    }

    @Test
    public void deserializeTest() {

        Double[][] deserialized = Utils.deserialize("4\t2\t9.0\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0");

        Double[][] expected = {
                {9.0, 6.0},
                {0.0, 1.0},
                {1.0, 0.0},
                {3.0, 6.0}
        };

        Assert.assertArrayEquals(expected, deserialized);
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongInputTest() {
        Utils.deserialize("1\t2\t1.0");
    }
}
