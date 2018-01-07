package org.unipi.matrixpnorm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MatrixColumnsMagnitudes extends Configured implements Tool {

    public static class MatrixColumnsMagnitudesMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Double[] row = Utils.deserializeArrayOfDoubles(value.toString());
            for (int c = 0; c < row.length; c++) {
                if (!row[c].equals(0.0)) {
                    context.write(new IntWritable(c), new DoubleWritable(row[c] * row[c]));
                }
            }
        }
    }

    public static class MatrixColumnsMagnitudesReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        private double maxValue = 0.0;
        private TreeMap<Integer, Double> magnitudes = new TreeMap<>();

        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            Double sum = 0.0;
            for (DoubleWritable val : values) {
                Double value = val.get();
                if (value > maxValue) {
                    maxValue = value;
                }
                sum += value;
            }
            magnitudes.put(key.get(), sum);
        }

        @Override
        public void cleanup(Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context context)
                throws IOException, InterruptedException {

            for(Map.Entry<Integer, Double> entry : magnitudes.entrySet()) {
                context.write(
                        new IntWritable(entry.getKey()),
                        new DoubleWritable(entry.getValue()/maxValue)
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MatrixColumnsMagnitudes(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "matrix columns norms");
        job.setJarByClass(MatrixColumnsMagnitudes.class);

        job.setMapperClass(MatrixColumnsMagnitudesMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(MatrixColumnsMagnitudesReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.waitForCompletion(true)) return 0;
        else return 1;
    }
}
