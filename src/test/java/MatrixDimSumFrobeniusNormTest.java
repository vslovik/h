import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.unipi.matrixpnorm.MatrixDimSumFrobeniusNorm;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;

import static java.lang.Math.log;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static java.lang.Math.sqrt;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

class MatrixDimSumFrobeniusNormTest {

    private MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormMapper mapper;
    private MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormReducer reducer;
    private MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormMapper.Context mapperContext;
    private MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormReducer.Context reducerContext;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        MatrixDimSumFrobeniusNorm dimSum = new MatrixDimSumFrobeniusNorm();
        mapper  = new MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormMapper();
        reducer = new MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormReducer();

        Configuration conf = new Configuration();

        mapperContext = mock(MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormMapper.Context.class, RETURNS_DEEP_STUBS);
        when(mapperContext.getConfiguration().getDouble("gamma", 1.0)).thenReturn(2 * log(4));
        when(mapperContext.getConfiguration().getDouble("0", 1.0)).thenReturn(4.0);
        when(mapperContext.getConfiguration().getDouble("1", 1.0)).thenReturn(4.0);
        when(mapperContext.getConfiguration().getDouble("2", 1.0)).thenReturn(4.0);
        when(mapperContext.getConfiguration().getDouble("3", 1.0)).thenReturn(4.0);

        reducerContext = mock(MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormReducer.Context.class, RETURNS_DEEP_STUBS);
        when(reducerContext.getConfiguration().getDouble("gamma", 1.0)).thenReturn(2 * log(4));
        when(reducerContext.getConfiguration().getDouble("0", 1.0)).thenReturn(4.0);
        when(reducerContext.getConfiguration().getDouble("1", 1.0)).thenReturn(4.0);
        when(reducerContext.getConfiguration().getDouble("2", 1.0)).thenReturn(4.0);
        when(reducerContext.getConfiguration().getDouble("3", 1.0)).thenReturn(4.0);
        when(reducerContext.getConfiguration().getDouble("max_squared_value", 1.0)).thenReturn(1.0);
    }

    @Test
    void mapTest() throws IOException, InterruptedException {

        //double gamma = 2 * log(4.0); // 2.7725887222398 gamma / 4.0 = 0.69314718055995

        mapper.map(new LongWritable(0), new Text("0.0\t1\t1.0\t0.0"), mapperContext);
        mapper.map(new LongWritable(1), new Text("1\t1.0\t0.0\t0.0"), mapperContext);
        mapper.map(new LongWritable(2), new Text("1.0\t0.0\t0.0\t1"), mapperContext);
        mapper.map(new LongWritable(3), new Text("0.0\t0.0\t1\t1.0"), mapperContext);

        mapper.map(new LongWritable(4), new Text("0.0\t1\t1.0\t0.0"), mapperContext);
        mapper.map(new LongWritable(5), new Text("1\t1.0\t0.0\t0.0"), mapperContext);
        mapper.map(new LongWritable(6), new Text("1.0\t0.0\t0.0\t1"), mapperContext);
        mapper.map(new LongWritable(7), new Text("0.0\t0.0\t1\t1.0"), mapperContext);

        verify(mapperContext, atMost(4)).write(new IntWritable(0), new DoubleWritable(1.0));
        verify(mapperContext, atMost(4)).write(new IntWritable(1), new DoubleWritable(1.0));
        verify(mapperContext, atMost(4)).write(new IntWritable(2), new DoubleWritable(1.0));
        verify(mapperContext, atMost(4)).write(new IntWritable(3), new DoubleWritable(1.0));
    }

    void reduceTest() throws IOException, InterruptedException {

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(1.0));
        values.add(new DoubleWritable(1.0));

        reducer.reduce(new IntWritable(0), values, reducerContext);
        reducer.reduce(new IntWritable(1), values, reducerContext);
        reducer.reduce(new IntWritable(2), values, reducerContext);
        reducer.reduce(new IntWritable(3), values, reducerContext);

        reducer.cleanup(reducerContext);

        verify(reducerContext).write(
                eq(NullWritable.get()),
                eq(new DoubleWritable(4.0 /sqrt(2 * log(4.0))))
        );
    }

}
