package org.unipi.matrixnorm;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.conf.Configured;
import java.util.TreeMap;
import java.util.Map;

public class HadoopMatrixNorm extends Configured implements Tool {

    public static class MatrixNormMapper extends Mapper<LongWritable, Text, MapperKey, MapperValue> {

        private int matrixIndex = 0;

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

        private double min = Double.MIN_VALUE;
        private double max = Double.MAX_VALUE;

        private int currentMatrixIndex = 0;
        private TreeMap<Integer, Double[]> treeMap = new TreeMap<>();

        private void setMinMax(Iterable<MapperValue> values) {
            for (MapperValue value : values) {
                if(value.colValue > max) {
                    max = value.colValue;
                }
                if(value.colValue < min) {
                    min = value.colValue;
                }
            }
        }

        private Double[] keepColumn(Iterable<MapperValue> values) {
            TreeMap<Integer, Double> colMap = new TreeMap<>();
            for (MapperValue value : values) {
                double newValue = (value.colValue - min) / (max - min);
                //double newValue = value.colValue;
                colMap.put(value.rowIndex,  newValue);
            }

            return colMap.values().toArray(new Double[colMap.values().size()]);
        }

        private Double[][] treeMapTo2DArray() {

            int rows = treeMap.entrySet().size();
            int cols = treeMap.firstEntry().getValue().length;

            Double[][] matrix = new Double[rows][cols];

            for(Map.Entry<Integer, Double[]> entry : treeMap.entrySet()) {

                Object[] a = new Integer[1];
                Integer b=1;
                a[0]=b;
                Integer[] c = (Integer[]) a;

                matrix[entry.getKey()] = entry.getValue();
            }

            return matrix;
        }

        private void emitMatrix(Context context) throws InterruptedException{

            try {
                String str = Utils.serialize(treeMapTo2DArray());
                context.write(
                        NullWritable.get(),
                        str
                );
            } catch(Exception e) {
                throw new InterruptedException();
            }
        }

        @Override
        public void reduce(MapperKey key, Iterable<MapperValue> values,
                              Context context) throws IOException, InterruptedException {

            if (key.flag == 0) {
                setMinMax(values);
            } else {
                treeMap.put(key.colIndex, keepColumn(values));
                min = Double.MIN_VALUE;
                max = Double.MAX_VALUE;
            }

            if (key.matrixIndex != currentMatrixIndex) {
                emitMatrix(context);
                currentMatrixIndex = key.matrixIndex;
                treeMap = new TreeMap<>();
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
