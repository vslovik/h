import org.unipi.matrixnorm.HadoopMatrixNorm;
import org.unipi.matrixnorm.MapperKey;
import org.unipi.matrixnorm.MapperValue;
import org.apache.hadoop.io.*;
import org.junit.Test;
import org.junit.Assert;

import org.unipi.matrixnorm.MapperKey;
import org.unipi.matrixnorm.MapperValue;
import org.apache.hadoop.io.*;
import org.mockito.Mockito.*;
import org.apache.hadoop.mapreduce.Mapper.Context;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.unipi.matrixnorm.Utils;

public class MatrixnormUtilsTest {

    @Test
    public void serializeTest() {

//        double[][] matrix = Array(
//                {9.0, 6.0},
//                Array(0.0, 1.0).toList,
//                Array(1.0, 0.0).toList,
//                Array(3.0, 6.0).toList
//        );
//
//        val serialized = generator.serialize(matrix)
//        val expected = "4\t2\t9.0\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0"
//
//        assert(expected == serialized, true);
    }

    @Test
    public void deserializeTest() {
//        val generator = new MatrixGenerator
//
//        val deserialized = generator.deserialize("4\t2\t9.0\t6.0\t0.0\t1.0\t1.0\t0.0\t3.0\t6.0")
//
//        val expected = Array(
//                Array(9.0, 6.0).toList,
//                Array(0.0, 1.0).toList,
//                Array(1.0, 0.0).toList,
//                Array(3.0, 6.0).toList
//        ).toList
//
//        assert(expected == deserialized, true)
    }

    @Test
    public void wrongInputTest() {
//        intercept[java.lang.IllegalArgumentException] {
//            (new MatrixGenerator).deserialize("1\t2\t1.0");
//        }
    }
}
