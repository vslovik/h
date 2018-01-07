package org.unipi.matrixrowgen;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.unipi.matrixpnorm.Utils;

import java.io.IOException;
import java.util.Random;

public class MatrixRowGenRecordReader extends RecordReader<NullWritable, Text> {

    private int numMatrixRowsToGenerate = 100;

    private int numMatrixCols = 100;
    private int createdRecords = 0;

    private NullWritable key = NullWritable.get();
    private Text value = new Text();

    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        this.numMatrixRowsToGenerate = context.getConfiguration().getInt(MatrixRowGenInputFormat.NUM_MATRIX_ROWS_PER_TASK, -1);
        this.numMatrixCols = context.getConfiguration().getInt(MatrixRowGenInputFormat.NUM_MATRIX_COLS, -1);
    }

    private Double[] generateMatrixRow(int cols) {

        Random rValues   = new Random();
        Random rNonZeros = new Random();

        double nonZerosLimit = 0.01;

        int minValue = 1;
        int maxValue = 100;

        int valueLimit = minValue + rValues.nextInt(maxValue - minValue);

        Double[] row = new Double[cols];

        for (int c = 0; c < cols; c++) {
            if(rNonZeros.nextDouble() < nonZerosLimit) {
                double leftLimit = 0D;
                double rightLimit = (double) valueLimit;
                row[c] = leftLimit + rValues.nextDouble() * (rightLimit - leftLimit);
            } else {
                row[c] = 0.0;
            }
        }

        return row;
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (createdRecords < numMatrixRowsToGenerate) {

            String serializedMatrixRow = Utils.serializeArrayOfDoubles(generateMatrixRow(numMatrixCols));

            value.set(serializedMatrixRow);
            createdRecords += 1;

            return true;
        }

        return false;
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return (float) createdRecords / (float) numMatrixRowsToGenerate;
    }

    public void close() throws IOException {
    }
}