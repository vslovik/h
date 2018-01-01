package org.unipi.matrixrowgen;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.unipi.matrixpnorm.Utils;

import java.io.IOException;
import java.util.Random;

public class MatrixRowGenRecordReader extends RecordReader<Integer, Text> {

    private int numMatrixRowsToGenerate = 100;

    private int numMatrixCols = 100;
    private int createdRecords = 0;

    private int key = 0;
    private Text value = new Text();

    private Random r = new Random();

    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        this.numMatrixRowsToGenerate = context.getConfiguration().getInt(MatrixRowGenInputFormat.NUM_MATRIX_ROWS_PER_TASK, -1);
        this.numMatrixCols = context.getConfiguration().getInt(MatrixRowGenInputFormat.NUM_MATRIX_COLS, -1);
    }

    private static Double[] generateMatrixRow(int valueLimit, int cols) {
        Double[] row = new Double[cols];

        for (int c = 0; c < cols; c++) {
            double leftLimit = 0D;
            double rightLimit = (double) valueLimit;
            row[c] = leftLimit + new Random().nextDouble() * (rightLimit - leftLimit);
        }

        return row;
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (createdRecords < numMatrixRowsToGenerate) {

            int minValue = 1;
            int maxValue = 100;

            int valueLimit = minValue + r.nextInt(maxValue - minValue);

            String serializedMatrixRow = Utils.serializeArrayOfDoubles(generateMatrixRow(valueLimit, numMatrixCols));

            key = createdRecords;
            value.set(serializedMatrixRow);
            createdRecords += 1;

            return true;
        }

        return false;
    }

    public Integer getCurrentKey() throws IOException, InterruptedException {
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