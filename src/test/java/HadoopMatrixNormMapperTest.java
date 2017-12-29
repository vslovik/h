import org.unipi.matrixnorm.HadoopMatrixNorm;
import org.unipi.matrixnorm.MapperKey;
import org.unipi.matrixnorm.MapperValue;

import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

import org.mockito.InOrder;

class HadoopMatrixNormMapperTest {

    private HadoopMatrixNorm.MatrixNormMapper mapper;
    private HadoopMatrixNorm.MatrixNormMapper.Context context;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        mapper = (new HadoopMatrixNorm.MatrixNormMapper());
        context = mock(HadoopMatrixNorm.MatrixNormMapper.Context.class);
    }

    @Test
    void wrongInput() throws IOException, InterruptedException {

        assertThrows(IOException.class,
                () -> {
                    mapper.map(null, new Text("1\t2\t1.0"), context);
                });

        InOrder inOrder = inOrder(context);
        inOrder.verify(context, never()).write(
                eq(new MapperKey(0, 0, 0)),
                eq(new MapperValue(0, 1.0))
        );
    }

    @Test
    void oneElementMatrixTest() throws IOException, InterruptedException {

        mapper.map(null, new Text("1\t1\t1.0"), context);

        InOrder inOrder = inOrder(context);

        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 0)),
                eq(new MapperValue( 0, 1.0))
        );

        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 1)),
                eq(new MapperValue( 0, 1.0))
        );
    }

    @Test
    void zeroMatrixTest() throws IOException, InterruptedException {

        mapper.map(null, new Text("2\t2\t0.0\t0.0\t0.0\t0.0"), context);

        InOrder inOrder = inOrder(context);

        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 0)),
                eq(new MapperValue( 0, 0.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 1)),
                eq(new MapperValue( 0, 0.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 0)),
                eq(new MapperValue( 0, 0.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 1)),
                eq(new MapperValue(0, 0.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 0)),
                eq(new MapperValue( 1, 0.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 1)),
                eq(new MapperValue( 1, 0.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 0)),
                eq(new MapperValue( 1, 0.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 1)),
                eq(new MapperValue( 1, 0.0))
        );
    }

    @Test
    void nonZeroMatrixTest() throws IOException, InterruptedException {

        mapper.map(
                null,
                new Text("4\t2\t9.0\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0"),
                context);

        InOrder inOrder = inOrder(context);

        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 0)),
                eq(new MapperValue( 0, 9.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 1)),
                eq(new MapperValue( 0, 9.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 0)),
                eq(new MapperValue( 0, 6.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 1)),
                eq(new MapperValue( 0, 6.0))
        );

        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 0)),
                eq(new MapperValue( 1, 0.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 1)),
                eq(new MapperValue( 1, 0.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 0)),
                eq(new MapperValue( 1, 1.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 1)),
                eq(new MapperValue( 1, 1.0))
        );

        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 0)),
                eq(new MapperValue( 2, 1.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 1)),
                eq(new MapperValue( 2, 1.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 0)),
                eq(new MapperValue( 2, 0.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 1)),
                eq(new MapperValue( 2, 0.0))
        );

        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 0)),
                eq(new MapperValue( 3, 3.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 0, 1)),
                eq(new MapperValue( 3, 3.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 0)),
                eq(new MapperValue( 3, 6.0))
        );
        inOrder.verify(context).write(
                eq(new MapperKey(0, 1, 1)),
                eq(new MapperValue( 3, 6.0))
        );
    }

}