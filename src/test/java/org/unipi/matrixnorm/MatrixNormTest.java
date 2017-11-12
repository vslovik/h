package org.unipi.matrixnorm;

import org.unipi.matrixnorm.MatrixNorm;
import org.junit.Assert;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class MatrixNormTest {
    @Test
    public void name() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MatrixNorm.print(new PrintStream(out));
        String s = out.toString();
        Assert.assertEquals("Hello, World!\n", s);
    }
}
