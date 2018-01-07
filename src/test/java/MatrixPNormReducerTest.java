import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(2.0));
        values.add(new DoubleWritable(3.0));
        values.add(new DoubleWritable(4.0));
        values.add(new DoubleWritable(5.0));

        reducer.reduce(
                new IntWritable(0),
                values,
                context
        );

        verify(context).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(pow(15.0, 0.5)))
        );
    }

    @Test
    void allOnesMatrixReduceTest() throws IOException, InterruptedException {

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(3.0));
        values.add(new DoubleWritable(3.0));
        values.add(new DoubleWritable(3.0));

        reducer.reduce(new IntWritable(0), values, context);

        verify(context).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(pow(9.0, 0.5)))
        );
    }

    @Test
    void unitaryMatrixReduceTest() throws IOException, InterruptedException {

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(1.0));

        reducer.reduce(new IntWritable(0), values, context);

        verify(context).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(pow(3.0, 0.5)))
        );
    }
}
