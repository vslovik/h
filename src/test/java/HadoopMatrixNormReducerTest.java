import org.unipi.matrixnorm.HadoopMatrixNorm;
import org.unipi.matrixnorm.MapperKey;
import org.unipi.matrixnorm.MapperValue;

import org.apache.hadoop.io.NullWritable;

import java.util.ArrayList;
import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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

    void oneElementMatrixTest() throws IOException, InterruptedException {

        InOrder inOrder = inOrder(context);

        ArrayList<MapperValue> values = new ArrayList<>();
        values.add(new MapperValue(0, 0, 0.0));

        reducer.reduce(
                eq(new MapperKey(0, 0, 0)),
                eq(values),
                context
        );

        reducer.reduce(
                eq(new MapperKey(0, 0, 1)),
                eq(values),
                context
        );

        inOrder.verify(context, never()).write(NullWritable.get(), "1\t1\t0.0");

        reducer.cleanup(context);

        inOrder.verify(context).write(NullWritable.get(), "1\t1\t0.0");
    }

    void emitZeroMatrixTest() throws IOException, InterruptedException {

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

        inOrder.verify(
                context,never()).write(NullWritable.get(),
                "4\t2\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0"
        );

        reducer.cleanup(context);

        inOrder.verify(context).write(
                NullWritable.get(),
                "4\t2\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0"
        );
    }

    void emitMatrixTest() throws IOException, InterruptedException {

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

        inOrder.verify(context, never()).write(
                NullWritable.get(),
                "4\t2\t1.0\t1.0\t0.0\t0.1667\t0.1111\t0.0\t0.3333\t1.0"
        );

        reducer.cleanup(context);

        inOrder.verify(context).write(
                NullWritable.get(),
                "4\t2\t1.0\t1.0\t0.0\t0.1667\t0.1111\t0.0\t0.3333\t1.0"
        );

    }
}
