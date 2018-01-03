import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.unipi.matrixpnorm.MatrixColumnsMagnitudes;

import java.io.IOException;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

class MatrixColumnsMagnitudesMapperTest {

    private MatrixColumnsMagnitudes.MatrixColumnsMagnitudesMapper mapper;
    private MatrixColumnsMagnitudes.MatrixColumnsMagnitudesMapper.Context context;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        mapper = new MatrixColumnsMagnitudes.MatrixColumnsMagnitudesMapper();
        context = mock(MatrixColumnsMagnitudes.MatrixColumnsMagnitudesMapper.Context.class, RETURNS_DEEP_STUBS);
        when(context.getConfiguration().getDouble("power", 2.0)).thenReturn(2.0);
    }

    @Test
    void wrongInput() throws IOException, InterruptedException {
        assertThrows(
                IOException.class,
                () -> mapper.map(0, new Text("blabla"), context)
        );
    }

    @Test
    void mapTest() throws IOException, InterruptedException {

        mapper.map(0, new Text("0.0\t1\t1.0\t0.0"), context);
        mapper.map(1, new Text("1\t1.0\t0.0\t0.0"), context);
        mapper.map(2, new Text("1.0\t0.0\t0.0\t1"), context);
        mapper.map(3, new Text("0.0\t0.0\t1\t1.0"), context);

        mapper.map(4, new Text("0.0\t1\t1.0\t0.0"), context);
        mapper.map(5, new Text("1\t1.0\t0.0\t0.0"), context);
        mapper.map(6, new Text("1.0\t0.0\t0.0\t1"), context);
        mapper.map(7, new Text("0.0\t0.0\t1\t1.0"), context);

        InOrder inOrder = inOrder(context);

        inOrder.verify(context, never()).write(
                eq(NullWritable.get()),
                eq("4\t4\t4\t4")
        );

        mapper.cleanup(context);

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq("4\t4\t4\t4")
        );
    }

    @Test
    void unitaryMarixTest() throws IOException, InterruptedException {

        mapper.map(0, new Text("1.0\t0.0\t0.0"), context);
        mapper.map(1, new Text("0.0\t1.0\t0.0"), context);
        mapper.map(2, new Text("0.0\t0.0\t1.0"), context);

        InOrder inOrder = inOrder(context);

        inOrder.verify(context, never()).write(
                eq(NullWritable.get()),
                eq("1\t1\t1")
        );

        mapper.cleanup(context);

        inOrder.verify(context).write(
                eq(NullWritable.get()),
                eq("1\t1\t1")
        );
    }
}