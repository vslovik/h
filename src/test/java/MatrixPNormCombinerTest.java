import org.apache.hadoop.io.NullWritable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.unipi.matrixpnorm.MatrixPNorm;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.ArrayList;

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

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(2.0));
        values.add(new DoubleWritable(3.0));
        values.add(new DoubleWritable(4.0));
        values.add(new DoubleWritable(5.0));

        combiner.reduce(new IntWritable(0), values, context);

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(15.0))
        );
    }

    @Test
    void allOnesMatrixReduceTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(1.0));

        combiner.reduce(new IntWritable(0), values, context);
        combiner.reduce(new IntWritable(1), values, context);
        combiner.reduce(new IntWritable(2), values, context);

        inOrder.verify(context, times(3)).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(3.0))
        );
    }

    @Test
    void unitaryMatrixReduceTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1.0));

        combiner.reduce(new IntWritable(0), values, context);
        combiner.reduce(new IntWritable(1), values, context);
        combiner.reduce(new IntWritable(2), values, context);

        inOrder.verify(context, times(3)).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(1.0))
        );
    }

}
