import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.unipi.matrixpnorm.MatrixColumnsMagnitudes;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

class MatrixColumnsMagnitudesTest {

    private MatrixColumnsMagnitudes.MatrixColumnsMagnitudesMapper mapper;
    private MatrixColumnsMagnitudes.MatrixColumnsMagnitudesReducer reducer;
    private MatrixColumnsMagnitudes.MatrixColumnsMagnitudesMapper.Context mapperContext;
    private MatrixColumnsMagnitudes.MatrixColumnsMagnitudesReducer.Context reducerContext;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        MatrixColumnsMagnitudes mcm =  new MatrixColumnsMagnitudes();
        mapper = mcm.new MatrixColumnsMagnitudesMapper();
        reducer = mcm.new MatrixColumnsMagnitudesReducer();
        mapperContext = mock(MatrixColumnsMagnitudes.MatrixColumnsMagnitudesMapper.Context.class);
        reducerContext = mock(MatrixColumnsMagnitudes.MatrixColumnsMagnitudesReducer.Context.class);
    }

    @Test
    void wrongInput() throws IOException, InterruptedException {
        assertThrows(
                IOException.class,
                () -> mapper.map(0, new Text("blabla"), mapperContext)
        );
    }

    @Test
    void mapReduceTest() throws IOException, InterruptedException {

        mapper.map(0, new Text("0.0\t1.0\t2.0\t3.0"), mapperContext);

        InOrder inOrder = inOrder(mapperContext);

        inOrder.verify(mapperContext).write(eq(1), eq(1.0));
        inOrder.verify(mapperContext).write(eq(2), eq(4.0));
        inOrder.verify(mapperContext).write(eq(3), eq(9.0));

        ArrayList<Double> values = new ArrayList<>();
        values.add(1.0);

        reducer.reduce(1, values, reducerContext);

        verify(reducerContext).write(eq(1), eq(1.0/9.0));
    }

}