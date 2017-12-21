package org.unipi.matrixnorm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HadoopMatrixNorm extends Configured implements Tool {

    public static class MatrixNormMapper extends Mapper<LongWritable, Text, MapperKey, MapperValue> {

        private int matrixIndex = 0;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {

                Double[][] matrix = Utils.deserialize(value.toString());

                for (int rowIndex = 0; rowIndex < matrix.length; rowIndex++) {
                    for (int columnIndex = 0; columnIndex < matrix[0].length; columnIndex++) {
                        context.write(
                                new MapperKey(matrixIndex, columnIndex, 0),
                                new MapperValue(matrixIndex, rowIndex, matrix[rowIndex][columnIndex])
                        );
                        context.write(
                                new MapperKey(matrixIndex, columnIndex, 1),
                                new MapperValue(matrixIndex, rowIndex, matrix[rowIndex][columnIndex])
                        );
                    }
                }

                matrixIndex += 1;

            } catch (IllegalArgumentException e) {
                throw new IOException();
            }
        }

    }

    public static class ColumnIndexPartitioner extends HashPartitioner<MapperKey, MapperValue> {

        public int getPartition(MapperKey key, MapperValue value, int numReduceTasks) {
            return super.getPartition(new MapperKey(key.matrixIndex, key.colIndex, 0), value, numReduceTasks);
        }
    }

    public static class MatrixNormReducer extends Reducer<MapperKey, MapperValue, NullWritable, String> {

        private int currentMatrixIndex = 0;
        private MatrixStorage storage = new MatrixStorage();

        private void emitMatrix(Context context) throws InterruptedException{

            try {
                String str = Utils.serialize(storage.get());
                context.write(NullWritable.get(), str);
            } catch(Exception e) {
                throw new InterruptedException();
            }
        }

        @Override
        public void reduce(MapperKey key, Iterable<MapperValue> values,
                              Context context) throws IOException, InterruptedException {

            if (key.flag == 0) {
                storage.keepMinMax(values);
            } else {
                storage.put(key.colIndex, values);
            }

            if (key.matrixIndex != currentMatrixIndex) {
                emitMatrix(context);
                currentMatrixIndex = key.matrixIndex;
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            emitMatrix(context);
        }

    }

    public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HadoopMatrixNorm(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "matrix normalization");
        job.setJarByClass(MatrixNormMapper.class);
        job.setMapperClass(MatrixNormMapper.class);
        job.setMapOutputKeyClass(MapperKey.class);
        job.setMapOutputValueClass(MapperValue.class);
        job.setPartitionerClass(ColumnIndexPartitioner.class);
        job.setReducerClass(MatrixNormReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(String.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (job.waitForCompletion(true)) return 0;
        else return 1;
    }
}
