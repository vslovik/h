package org.unipi.matrixpnorm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.unipi.matrixnorm.HadoopMatrixNorm;
import org.unipi.matrixnorm.MapperKey;
import org.unipi.matrixnorm.MapperValue;
import org.unipi.matrixnorm.Utils;

import java.io.IOException;

public class PNorm extends Configured implements Tool {
    public static class PNormMapper extends Mapper<LongWritable, Text, MapperKey, MapperValue> {

        private int matrixIndex = 0;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {

                Double[][] matrix = Utils.deserialize(value.toString());

                for (int rowIndex = 0; rowIndex < matrix.length; rowIndex++) {
                    for (int columnIndex = 0; columnIndex < matrix[0].length; columnIndex++) {
                        context.write(
                                new MapperKey(matrixIndex, columnIndex, 0),
                                new MapperValue(rowIndex, matrix[rowIndex][columnIndex])
                        );
                        context.write(
                                new MapperKey(matrixIndex, columnIndex, 1),
                                new MapperValue(rowIndex, matrix[rowIndex][columnIndex])
                        );
                    }
                }

                matrixIndex += 1;

            } catch (IllegalArgumentException e) {
                throw new IOException();
            }
        }

    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "matrix p-norm");
        job.setJarByClass(HadoopMatrixNorm.MatrixNormMapper.class);


        job.setMapperClass(HadoopMatrixNorm.MatrixNormMapper.class);
        job.setMapOutputKeyClass(MapperKey.class);
        job.setMapOutputValueClass(MapperValue.class);
        job.setPartitionerClass(HadoopMatrixNorm.ColumnIndexPartitioner.class);
        job.setReducerClass(HadoopMatrixNorm.MatrixNormReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(String.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (job.waitForCompletion(true)) return 0;
        else return 1;
    }
}
