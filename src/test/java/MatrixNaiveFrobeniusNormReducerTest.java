import org.apache.hadoop.io.NullWritable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.unipi.matrixpnorm.MatrixNaiveFrobeniusNorm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import static java.lang.Math.pow;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

class MatrixNaiveFrobeniusNormReducerTest {

    private MatrixNaiveFrobeniusNorm.MatrixNaiveFrobeniusNormReducer reducer;
    private MatrixNaiveFrobeniusNorm.MatrixNaiveFrobeniusNormReducer.Context context;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        reducer = new MatrixNaiveFrobeniusNorm.MatrixNaiveFrobeniusNormReducer();
        context = mock(MatrixNaiveFrobeniusNorm.MatrixNaiveFrobeniusNormReducer.Context.class);
    }

    @Test
    void reduceTest() throws IOException, InterruptedException {

        Random r = new Random();
        Integer key = r.nextInt();

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(2.0));
        values.add(new DoubleWritable(3.0));
        values.add(new DoubleWritable(4.0));
        values.add(new DoubleWritable(5.0));

        reducer.reduce(
                new IntWritable(key),
                values,
                context
        );

        verify(context, never()).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(pow(15.0, 0.5)))
        );

        reducer.cleanup(context);

        verify(context).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(pow(15.0, 0.5)))
        );
    }

    @Test
    void allOnesMatrixReduceTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(1.0));

        reducer.reduce(new IntWritable(0), values, context);
        reducer.reduce(new IntWritable(1), values, context);
        reducer.reduce(new IntWritable(3), values, context);

        inOrder.verify(context, never()).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(pow(9.0, 0.5)))
        );

        reducer.cleanup(context);

        verify(context).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(pow(9.0, 0.5)))
        );
    }

    @Test
    void unitaryMatrixReduceTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1.0));

        reducer.reduce(new IntWritable(0), values, context);
        reducer.reduce(new IntWritable(1), values, context);
        reducer.reduce(new IntWritable(2), values, context);

        inOrder.verify(context, never()).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(pow(3.0, 0.5)))
        );

        reducer.cleanup(context);

        verify(context).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(pow(3.0, 0.5)))
        );
    }

}
