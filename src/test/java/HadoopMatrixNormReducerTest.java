import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito.*;
import org.unipi.matrixnorm.HadoopMatrixNorm;
import org.unipi.matrixnorm.MapperKey;
import org.unipi.matrixnorm.MapperValue;

import java.util.ArrayList;

import java.io.IOException;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

public class HadoopMatrixNormReducerTest {

    private HadoopMatrixNorm.MatrixNormReducer reducer;
    private Reducer.Context context;

    @Before
    public void init() throws IOException, InterruptedException {
        reducer = new HadoopMatrixNorm.MatrixNormReducer();
        context = mock(Reducer.Context.class);
    }

    @Test
    public void oneElementMatrixTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<MapperValue> values = new ArrayList<>();
        values.add(new MapperValue(0, 0, 0.0));

        reducer.reduce(
                new MapperKey(0, 0, 0),
                values,
                context
        );

        reducer.reduce(
                new MapperKey(0, 0, 1),
                values,
                context
        );

        //inOrder.verify(context, never()).write(NullWritable.get(), "1\t1\t0.0");

        reducer.cleanup(context);

        inOrder.verify(context).write(NullWritable.get(), "1\t1\t0.0");
    }

    @Test
    public void emitZeroMatrixTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<MapperValue> values0 = new ArrayList<>();
        values0.add(new MapperValue(0, 0, 0.0));
        values0.add(new MapperValue(0, 1, 0.0));
        values0.add(new MapperValue(0, 2, 0.0));
        values0.add(new MapperValue(0, 3, 0.0));

        reducer.reduce(new MapperKey(0, 0, 0), values0, context);

        ArrayList<MapperValue> values1 = new ArrayList<>();
        values1.add(new MapperValue(0, 0, 0.0));
        values1.add(new MapperValue(0, 1, 0.0));
        values1.add(new MapperValue(0, 2, 0.0));
        values1.add(new MapperValue(0, 3, 0.0));

        reducer.reduce(new MapperKey(0, 0, 1), values1, context);

        ArrayList<MapperValue> values2 = new ArrayList<>();
        values2.add(new MapperValue(0, 0, 0.0));
        values2.add(new MapperValue(0, 1, 0.0));
        values2.add(new MapperValue(0, 2, 0.0));
        values2.add(new MapperValue(0, 3, 0.0));

        reducer.reduce(new MapperKey(0, 1, 0), values2, context);

        ArrayList<MapperValue> values3 = new ArrayList<>();
        values3.add(new MapperValue(0, 0, 0.0));
        values3.add(new MapperValue(0, 1, 0.0));
        values3.add(new MapperValue(0, 2, 0.0));
        values3.add(new MapperValue(0, 3, 0.0));

        reducer.reduce(new MapperKey(0, 1, 1), values3, context);

        //inOrder.verify(context, never()).write(NullWritable.get(), "4\t2\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0");

        reducer.cleanup(context);

        inOrder.verify(context).write(
                NullWritable.get(),
                "4\t2\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0"
        );
    }

    @Test
    public void emitMatrixTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<MapperValue> values0 = new ArrayList<>();
        values0.add(new MapperValue(0, 0, 9.0));
        values0.add(new MapperValue(0, 1, 0.0));
        values0.add(new MapperValue(0, 2, 1.0));
        values0.add(new MapperValue(0, 3, 3.0));

        reducer.reduce(new MapperKey(0, 0, 0), values0, context);

        ArrayList<MapperValue> values1 = new ArrayList<>();
        values1.add(new MapperValue(0, 0, 9.0));
        values1.add(new MapperValue(0, 1, 0.0));
        values1.add(new MapperValue(0, 2, 1.0));
        values1.add(new MapperValue(0, 3, 3.0));

        reducer.reduce(new MapperKey(0, 0, 1), values1, context);

        ArrayList<MapperValue> values2 = new ArrayList<>();

        values2.add(new MapperValue(0, 0, 6.0));
        values2.add(new MapperValue(0, 1, 1.0));
        values2.add(new MapperValue(0, 2, 0.0));
        values2.add(new MapperValue(0, 3, 6.0));

        reducer.reduce(new MapperKey(0, 1, 0), values2, context);

        ArrayList<MapperValue> values3 = new ArrayList<>();
        values3.add(new MapperValue(0, 0, 6.0));
        values3.add(new MapperValue(0, 1, 1.0));
        values3.add(new MapperValue(0, 2, 0.0));
        values3.add(new MapperValue(0, 3, 6.0));

        reducer.reduce(new MapperKey(0, 1, 1), values3, context);

        //inOrder.verify(context, never()).write(NullWritable.get(), "4\t2\t1.0\t1.0\t0.0\t0.1667\t0.1111\t0.0\t0.3333\t1.0");

        reducer.cleanup(context);

        inOrder.verify(context).write(NullWritable.get(), "4\t2\t1.0\t1.0\t0.0\t0.1667\t0.1111\t0.0\t0.3333\t1.0");

    }
}
