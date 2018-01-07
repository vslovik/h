import org.unipi.matrixnorm.HadoopMatrixNorm;
import org.unipi.matrixnorm.MapperKey;
import org.unipi.matrixnorm.MapperValue;

import org.apache.hadoop.io.NullWritable;

import java.util.ArrayList;
import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;

import org.mockito.InOrder;

class HadoopMatrixNormReducerTest {

    private HadoopMatrixNorm.MatrixNormReducer reducer;
    private HadoopMatrixNorm.MatrixNormReducer.Context context;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        reducer = new HadoopMatrixNorm.MatrixNormReducer();
        context = mock(HadoopMatrixNorm.MatrixNormReducer.Context.class);
    }

    @Test
    void oneElementMatrixTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<MapperValue> values = new ArrayList<>();
        values.add(new MapperValue(0, 0.0));

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

        inOrder.verify(context, never()).write(
                eq(NullWritable.get()),
                eq("1\t1\t0")
        );

        reducer.cleanup(context);

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq("1\t1\t0")
        );
    }

    @Test
    void emitZeroMatrixTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<MapperValue> values0 = new ArrayList<>();
        values0.add(new MapperValue(0, 0.0));
        values0.add(new MapperValue(1, 0.0));
        values0.add(new MapperValue(2, 0.0));
        values0.add(new MapperValue(3, 0.0));

        reducer.reduce(new MapperKey(0, 0, 0), values0, context);

        ArrayList<MapperValue> values1 = new ArrayList<>();
        values1.add(new MapperValue(0, 0.0));
        values1.add(new MapperValue(1, 0.0));
        values1.add(new MapperValue(2, 0.0));
        values1.add(new MapperValue(3, 0.0));

        reducer.reduce(new MapperKey(0, 0, 1), values1, context);

        ArrayList<MapperValue> values2 = new ArrayList<>();
        values2.add(new MapperValue(0, 0.0));
        values2.add(new MapperValue(1, 0.0));
        values2.add(new MapperValue(2, 0.0));
        values2.add(new MapperValue(3, 0.0));

        reducer.reduce(new MapperKey(0, 1, 0), values2, context);

        ArrayList<MapperValue> values3 = new ArrayList<>();
        values3.add(new MapperValue(0, 0.0));
        values3.add(new MapperValue(1, 0.0));
        values3.add(new MapperValue(2, 0.0));
        values3.add(new MapperValue(3, 0.0));

        reducer.reduce(new MapperKey(0, 1, 1), values3, context);

        inOrder.verify(
                context,never()).write(
                eq(NullWritable.get()),
                eq("4\t2\t0\t0\t0\t0\t0\t0\t0\t0")
        );

        reducer.cleanup(context);

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq("4\t2\t0\t0\t0\t0\t0\t0\t0\t0")
        );
    }

    @Test
    void emitMatrixTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<MapperValue> values0 = new ArrayList<>();
        values0.add(new MapperValue(0, 9.0));
        values0.add(new MapperValue(1, 0.0));
        values0.add(new MapperValue(2, 1.0));
        values0.add(new MapperValue(3, 3.0));

        reducer.reduce(new MapperKey(0, 0, 0), values0, context);

        ArrayList<MapperValue> values1 = new ArrayList<>();
        values1.add(new MapperValue(0, 9.0));
        values1.add(new MapperValue(1, 0.0));
        values1.add(new MapperValue(2, 1.0));
        values1.add(new MapperValue(3, 3.0));

        reducer.reduce(new MapperKey(0, 0, 1), values1, context);

        ArrayList<MapperValue> values2 = new ArrayList<>();

        values2.add(new MapperValue(0, 6.0));
        values2.add(new MapperValue(1, 1.0));
        values2.add(new MapperValue(2, 0.0));
        values2.add(new MapperValue(3, 6.0));

        reducer.reduce(new MapperKey(0, 1, 0), values2, context);

        ArrayList<MapperValue> values3 = new ArrayList<>();
        values3.add(new MapperValue(0, 6.0));
        values3.add(new MapperValue(1, 1.0));
        values3.add(new MapperValue(2, 0.0));
        values3.add(new MapperValue(3, 6.0));

        reducer.reduce(new MapperKey(0, 1, 1), values3, context);

        inOrder.verify(context, never()).write(
                eq(NullWritable.get()),
                eq("4\t2\t1\t1\t0\t0.1667\t0.1111\t0\t0.3333\t1")
        );

        reducer.cleanup(context);

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq("4\t2\t1\t1\t0\t0.1667\t0.1111\t0\t0.3333\t1")
        );

    }
}
