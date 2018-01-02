import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.unipi.matrixpnorm.MatrixNaiveFrobeniusNorm;

import java.io.IOException;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

class MatrixNaiveFrobeniusNormMapperTest {

    private MatrixNaiveFrobeniusNorm.MatrixNaiveFrobeniusNormMapper mapper;
    private MatrixNaiveFrobeniusNorm.MatrixNaiveFrobeniusNormMapper.Context context;

    @BeforeEach
    void init() throws IOException, InterruptedException {
        mapper = new MatrixNaiveFrobeniusNorm.MatrixNaiveFrobeniusNormMapper();
        context = mock(MatrixNaiveFrobeniusNorm.MatrixNaiveFrobeniusNormMapper.Context.class);
    }

    @Test
    void reduceTest() throws IOException, InterruptedException {

        mapper.map(0, new Text("1.65\t1\t2.0\t0.0"), context);

        InOrder inOrder = inOrder(context);

        inOrder.verify(context).write(
                eq(0),
                eq(1.65*1.65)
        );

        inOrder.verify(context).write(
                eq(1),
                eq(1.0 * 1.0)
        );

        inOrder.verify(context).write(
                eq(2),
                eq(2.0 * 2.0)
        );

        inOrder.verify(context, never()).write(
                eq(3),
                eq(0.0)
        );

    }

}
