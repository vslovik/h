import org.apache.hadoop.io.NullWritable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.unipi.matrixpnorm.MatrixPNorm;

import java.io.IOException;
import java.util.ArrayList;

import static java.lang.Math.pow;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class MatrixPNormCombinerTest {

    private MatrixPNorm.MatrixPNormCombiner combiner;
    private MatrixPNorm.MatrixPNormCombiner.Context context;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        combiner = new MatrixPNorm.MatrixPNormCombiner();
        context = mock(MatrixPNorm.MatrixPNormCombiner.Context.class);
    }

    @Test
    void reduceTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<Double> values = new ArrayList<>();
        values.add(1.0);
        values.add(2.0);
        values.add(3.0);
        values.add(4.0);
        values.add(5.0);

        combiner.reduce(0, values, context);

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq(15.0)
        );
    }

    @Test
    void allOnesMatrixReduceTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<Double> values = new ArrayList<>();
        values.add(1.0);
        values.add(1.0);
        values.add(1.0);

        combiner.reduce(0, values, context);
        combiner.reduce(1, values, context);
        combiner.reduce(2, values, context);

        inOrder.verify(context, times(3)).write(
                eq(NullWritable.get()),
                eq(3.0)
        );
    }

    @Test
    void unitaryMatrixReduceTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<Double> values = new ArrayList<>();
        values.add(1.0);

        combiner.reduce(0, values, context);
        combiner.reduce(1, values, context);
        combiner.reduce(2, values, context);

        inOrder.verify(context, times(3)).write(
                eq(NullWritable.get()),
                eq(1.0)
        );
    }

}
