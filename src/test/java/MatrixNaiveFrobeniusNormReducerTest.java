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

        InOrder inOrder = inOrder(context);

        ArrayList<Double> values = new ArrayList<>();
        values.add(1.0);
        values.add(2.0);
        values.add(3.0);
        values.add(4.0);
        values.add(5.0);

        reducer.reduce(
                key,
                values,
                context
        );

        inOrder.verify(context, never()).write(
                eq(NullWritable.get()),
                eq( pow(15.0, 0.5))
        );

        reducer.cleanup(context);

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq( pow(15.0, 0.5))
        );
    }

}
