package org.unipi.matrixpnorm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.unipi.matrixnorm.HadoopMatrixNorm;

import java.io.IOException;
import static java.lang.Math.sqrt;

public class MatrixColumnsMagnitudes extends Configured implements Tool {

    private double maxValue = 0.0;

    public class MatrixColumnsMagnitudesMapper extends Mapper<Integer, Text, Integer, Double> {

        @Override
        public void map(Integer key, Text value, Context context) throws IOException, InterruptedException {

            try {
                Double[] row = Utils.deserializeArrayOfDoubles(value.toString());

                for(int c = 0; c < row.length; c++) {
                    if(row[c] > maxValue) {
                        maxValue = row[c];
                    }
                    if(!row[c].equals(0.0)) {
                        context.write(c, row[c] * row[c]);
                    }
                }

            } catch (IllegalArgumentException e) {
                throw new IOException();
            }
        }
    }

    public class MatrixColumnsMagnitudesReducer extends Reducer<Integer, Double, Integer, Double> {

        public void reduce(Integer key, Iterable<Double> values, Context context)
                throws IOException, InterruptedException {

            Double sum = 0.0;
            for (Double val : values) {
                sum += val;
            }

            Double magnitude = sqrt(sum)/maxValue;
            magnitude *= magnitude;

            context.write(key, magnitude);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HadoopMatrixNorm(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "matrix columns norms");
        job.setJarByClass(MatrixColumnsMagnitudes.class);

        job.setMapperClass(MatrixColumnsMagnitudesMapper.class);

        job.setMapOutputKeyClass(Integer.class);
        job.setMapOutputValueClass(Double.class);

        job.setOutputKeyClass(Integer.class);
        job.setOutputValueClass(Double.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.waitForCompletion(true)) return 0;
        else return 1;
    }
}
