package org.unipi.matrixpnorm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.unipi.matrixnorm.HadoopMatrixNorm;

import java.io.IOException;

import static java.lang.Math.pow;
import static java.lang.Math.abs;

public class MatrixPNorm extends Configured implements Tool {

    public static class MatrixPNormMapper extends Mapper<Integer, Text, Integer, Double> {

        @Override
        public void map(Integer key, Text value, Context context) throws IOException, InterruptedException {
            try {

                Double p = context.getConfiguration().getDouble("power", 2.0);

                int rowIndex = key;
                Double[] row = Utils.deserializeArrayOfDoubles(value.toString());

                for (Double colValue: row) {
                    if (!colValue.equals(0.0)) {
                        context.write(rowIndex, pow(abs(colValue), p));
                    }
                }

            } catch (IllegalArgumentException e) {
                throw new IOException();
            }
        }
    }

    public static class MatrixPNormCombiner extends Reducer<Integer, Double, NullWritable, Double> {

        @Override
        public void reduce(Integer key, Iterable<Double> values,
                           Context context) throws IOException, InterruptedException {

            Double sum = 0.0;
            for (Double value : values) {
                sum += value;
            }

            context.write(NullWritable.get(), sum);
        }
    }

    public static class MatrixPNormReducer extends Reducer<NullWritable, Double, NullWritable, Double> {

        @Override
        public void reduce(NullWritable key, Iterable<Double> values,
                           Context context) throws IOException, InterruptedException {

            Double p = context.getConfiguration().getDouble("power", 2.0);

            Double sum = 0.0;
            for (Double value : values) {
                sum += value;
            }

            context.write(NullWritable.get(), pow(sum, 1.0 / p));
        }
    }

    public void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MatrixPNorm(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("USAGE : [input path] [output path] [p: double number or 'infinity' key word] ");
            return 1;
        } else {

            Double p = Double.parseDouble(args[2]);
            Configuration conf = new Configuration();
            conf.setDouble("power", p);
            Job job = Job.getInstance(conf, "matrix p-norm");
            job.setJarByClass(MatrixPNorm.class);

            job.setMapperClass(MatrixPNorm.MatrixPNormMapper.class);
            job.setCombinerClass(MatrixPNormCombiner.class);

            job.setMapOutputKeyClass(Integer.class);
            job.setMapOutputValueClass(Double.class);
            job.setReducerClass(MatrixPNormReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(String.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            if (job.waitForCompletion(true)) return 0;
            else return 1;
        }
    }
}
