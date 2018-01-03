import org.apache.hadoop.io.NullWritable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.unipi.matrixpnorm.MatrixPNorm;

import java.io.IOException;
import java.util.ArrayList;

import static java.lang.Math.pow;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

class MatrixPNormReducerTest {

    private MatrixPNorm.MatrixPNormReducer reducer;
    private MatrixPNorm.MatrixPNormReducer.Context context;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        reducer = new MatrixPNorm.MatrixPNormReducer();
        context = mock(MatrixPNorm.MatrixPNormReducer.Context.class, RETURNS_DEEP_STUBS);
        when(context.getConfiguration().getDouble("power", 2.0)).thenReturn(2.0);
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

        reducer.reduce(
                NullWritable.get(),
                values,
                context
        );

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq(pow(15.0, 0.5))
        );
    }

    @Test
    void allOnesMatrixReduceTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<Double> values = new ArrayList<>();
        values.add(3.0);
        values.add(3.0);
        values.add(3.0);

        reducer.reduce(NullWritable.get(), values, context);

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq(pow(9.0, 0.5))
        );
    }

    @Test
    void unitaryMatrixReduceTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<Double> values = new ArrayList<>();
        values.add(1.0);
        values.add(1.0);
        values.add(1.0);

        reducer.reduce(NullWritable.get(), values, context);

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq(pow(3.0, 0.5))
        );
    }
}
