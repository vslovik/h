package org.unipi.matrixpnorm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import static java.lang.Math.pow;
import static java.lang.Math.abs;

public class MatrixPNorm extends Configured implements Tool {

    public static class MatrixPNormMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Double p = context.getConfiguration().getDouble("power", 2.0);

            Double[] row = Utils.deserializeArrayOfDoubles(value.toString());

            for (Double colValue : row) {
                if (!colValue.equals(0.0)) {
                    context.write(
                            new IntWritable((int) key.get()),
                            new DoubleWritable(pow(abs(colValue), p)));
                }
            }
        }
    }

    public static class MatrixPNormCombiner extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {

            Double sum = 0.0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }

            context.write(new IntWritable(0), new DoubleWritable(sum));
        }
    }

    public static class MatrixPNormReducer extends Reducer<IntWritable, DoubleWritable, NullWritable, DoubleWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {

            Double p = context.getConfiguration().getDouble("power", 2.0);

            Double sum = 0.0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }

            context.write(NullWritable.get(), new DoubleWritable(pow(sum, 1.0 / p)));
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MatrixPNorm(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("USAGE : [input path] [output path] [p: double number] ");
            return 1;
        } else {

            Double p = Double.parseDouble(args[2]);
            Configuration conf = new Configuration();
            conf.setDouble("power", p);
            Job job = Job.getInstance(conf, "matrix p-norm");

            job.setJarByClass(MatrixPNorm.class);

            job.setMapperClass(MatrixPNormMapper.class);
            job.setCombinerClass(MatrixPNormCombiner.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setReducerClass(MatrixPNormReducer.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Double.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            if (job.waitForCompletion(true)) return 0;
            else return 1;
        }
    }
}
