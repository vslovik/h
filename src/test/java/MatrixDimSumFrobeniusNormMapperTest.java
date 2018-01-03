import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.unipi.matrixpnorm.MatrixDimSumFrobeniusNorm;

import java.io.IOException;
import java.util.ArrayList;

import static java.lang.Math.log;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static java.lang.Math.sqrt;

class MatrixDimSumFrobeniusNormMapperTest {

    private MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormMapper mapper;
    private MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormReducer reducer;
    private MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormMapper.Context mapperContext;
    private MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormReducer.Context reducerContext;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        MatrixDimSumFrobeniusNorm dimSum = new MatrixDimSumFrobeniusNorm();
        mapper  = dimSum.new MatrixDimSumFrobeniusNormMapper();
        reducer = dimSum.new MatrixDimSumFrobeniusNormReducer();

        mapperContext = mock(MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormMapper.Context.class, RETURNS_DEEP_STUBS);
        when(mapperContext.getConfiguration().get("magnitudes_serialized")).thenReturn("4\t4\t4\t4");

        reducerContext = mock(MatrixDimSumFrobeniusNorm.MatrixDimSumFrobeniusNormReducer.Context.class, RETURNS_DEEP_STUBS);
        when(reducerContext.getConfiguration().get("magnitudes_serialized")).thenReturn("4\t4\t4\t4");
    }

    @Test
    void mapTest() throws IOException, InterruptedException {

        //double gamma = 2 * log(4.0); // 2.7725887222398 gamma / 4.0 = 0.69314718055995

        mapper.map(0, new Text("0.0\t1\t1.0\t0.0"), mapperContext);
        mapper.map(1, new Text("1\t1.0\t0.0\t0.0"), mapperContext);
        mapper.map(2, new Text("1.0\t0.0\t0.0\t1"), mapperContext);
        mapper.map(3, new Text("0.0\t0.0\t1\t1.0"), mapperContext);

        mapper.map(4, new Text("0.0\t1\t1.0\t0.0"), mapperContext);
        mapper.map(5, new Text("1\t1.0\t0.0\t0.0"), mapperContext);
        mapper.map(6, new Text("1.0\t0.0\t0.0\t1"), mapperContext);
        mapper.map(7, new Text("0.0\t0.0\t1\t1.0"), mapperContext);

        verify(mapperContext, atMost(4)).write(0, 1.0);
        verify(mapperContext, atMost(4)).write(1, 1.0);
        verify(mapperContext, atMost(4)).write(2, 1.0);
        verify(mapperContext, atMost(4)).write(3, 1.0);
    }

    @Test
    void reduceTest() throws IOException, InterruptedException {

        ArrayList<Double> values = new ArrayList<>();
        values.add(1.0);
        values.add(1.0);
        values.add(1.0);
        values.add(1.0);

        reducer.reduce(0, values, reducerContext);
        reducer.reduce(1, values, reducerContext);
        reducer.reduce(2, values, reducerContext);
        reducer.reduce(3, values, reducerContext);

        reducer.cleanup(reducerContext);

        verify(reducerContext).write(
                eq(NullWritable.get()),
                eq(4.0 /sqrt(2 * log(4.0)))
        );
    }

}
