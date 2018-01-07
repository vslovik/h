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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

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
                NumberFormatException.class,
                () -> mapper.map(new LongWritable(0), new Text("blabla"), mapperContext)
        );
    }

    @Test
    void mapReduceTest() throws IOException, InterruptedException {

        mapper.map(new LongWritable(0), new Text("0.0\t1.0\t2.0\t3.0"), mapperContext);

        InOrder inOrder = inOrder(mapperContext);

        inOrder.verify(mapperContext).write(eq(new IntWritable(1)), eq(new DoubleWritable(1.0)));
        inOrder.verify(mapperContext).write(eq(new IntWritable(2)), eq(new DoubleWritable(4.0)));
        inOrder.verify(mapperContext).write(eq(new IntWritable(3)), eq(new DoubleWritable(9.0)));

        ArrayList<DoubleWritable> values = new ArrayList<>();
        values.add(new DoubleWritable(1.0));

        reducer.reduce(new IntWritable(1), values, reducerContext);

        verify(reducerContext).write(eq(new IntWritable(1)), eq(new DoubleWritable(1.0/9.0)));
    }

}