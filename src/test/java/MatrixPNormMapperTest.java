import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import java.util.Random;
import org.unipi.matrixpnorm.MatrixPNorm;

import java.io.IOException;

import static java.lang.Math.abs;
import static java.lang.Math.pow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

class MatrixPNormMapperTest {

    private MatrixPNorm.MatrixPNormMapper mapper;
    private MatrixPNorm.MatrixPNormMapper.Context context;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        mapper = (new MatrixPNorm.MatrixPNormMapper());
        context = mock(MatrixPNorm.MatrixPNormMapper.Context.class, RETURNS_DEEP_STUBS);
        when(context.getConfiguration().getDouble("power", 2.0)).thenReturn(2.0);
    }

    @Test
    void wrongInput() throws IOException, InterruptedException {

        assertThrows(IOException.class,
                () -> {
                    mapper.map(0, new Text("blabla"), context);
                });
    }

    @Test
    void mapTest() throws IOException, InterruptedException {

        Random r = new Random();
        Integer key = r.nextInt();

        mapper.map(key, new Text("1.65\t1\t2.0\t0.0"), context);

        InOrder inOrder = inOrder(context);

        inOrder.verify(context).write(
                eq(key),
                eq(pow(abs(1.65), 2.0))
        );

        inOrder.verify(context).write(
                eq(key),
                eq(pow(abs(1.0), 2.0))
        );

        inOrder.verify(context).write(
                eq(key),
                eq(pow(abs(2.0), 2.0))
        );

        inOrder.verify(context, never()).write(
                eq(key),
                eq(pow(abs(0.0), 2.0))
        );
    }

    @Test
    void allOnesMatrixMapTest() throws IOException, InterruptedException {

        mapper.map(0, new Text("1.0\t1.0\t1.0"), context);
        mapper.map(1, new Text("1.0\t1.0\t1.0"), context);
        mapper.map(2, new Text("1.0\t1.0\t1.0"), context);

        InOrder inOrder = inOrder(context);

        inOrder.verify(context, times(3))
                .write(eq(0), eq(1.0));
        inOrder.verify(context, times(3))
                .write(eq(1), eq(1.0));
        inOrder.verify(context, times(3))
                .write(eq(2), eq(1.0));
    }

    @Test
    void unitaryMatrixMapTest() throws IOException, InterruptedException {

        mapper.map(0, new Text("1.0\t0.0\t0.0"), context);
        mapper.map(1, new Text("0.0\t1.0\t0.0"), context);
        mapper.map(2, new Text("0.0\t0.0\t1.0"), context);

        InOrder inOrder = inOrder(context);

        inOrder.verify(context).write(eq(0), eq(1.0));
        inOrder.verify(context).write(eq(1), eq(1.0));
        inOrder.verify(context).write(eq(2), eq(1.0));
    }

    @Test
    void zeroMatrixMapTest() throws IOException, InterruptedException {

        mapper.map(0, new Text("0.0\t0.0\t0.0"), context);
        mapper.map(1, new Text("0.0\t0.0\t0.0"), context);
        mapper.map(2, new Text("0.0\t0.0\t0.0"), context);

        InOrder inOrder = inOrder(context);

        inOrder.verify(context, never()).write(eq(0), eq(0.0));
        inOrder.verify(context, never()).write(eq(1), eq(0.0));
        inOrder.verify(context, never()).write(eq(2), eq(0.0));
    }
}