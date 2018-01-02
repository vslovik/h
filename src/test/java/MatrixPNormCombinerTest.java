import org.apache.hadoop.io.NullWritable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.unipi.matrixpnorm.MatrixPNorm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

class MatrixPNormCombinerTest {

    private MatrixPNorm.MatrixPNormCombiner combiner;
    private MatrixPNorm.MatrixPNormCombiner.Context context;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        combiner = new MatrixPNorm.MatrixPNormCombiner();
        context = mock(MatrixPNorm.MatrixPNormCombiner.Context.class);
    }

    @Test
    void reduceTest() {

        Random r = new Random();
        Integer key = r.nextInt();

        InOrder inOrder = inOrder(context);

        ArrayList<Double> values = new ArrayList<>();
        values.add(1.0);
        values.add(2.0);
        values.add(3.0);
        values.add(4.0);
        values.add(5.0);

//        combiner.reduce(1, values, context);
//
//        inOrder.verify(context).write(
//                eq(key),
//                eq(15.0)
//        );

    }

}
