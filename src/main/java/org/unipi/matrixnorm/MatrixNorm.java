package org.unipi.matrixnorm;

import java.util.*;

public class MatrixNorm {

    private Stack<MapInputFormat> mapInQueue;
    private Stack<MapOutputFormat> mapOutQueue;

    private Stack<ReduceInputPair> reduceInQueue;
    private Stack<ReduceOutputFormat> reduceOutQueue;

    private int rows;
    private int cols;

    private HashMap<Tuple, List<Tuple<Integer,Double>>> storage;

    public double[][] normalize(Double[][] matrix)
    {
        return this.schedule(matrix).map().combine().reduce().compose();
    }

    private MatrixNorm schedule(Double[][] matrix)
    {
        mapInQueue = new Stack<>();
        rows = matrix.length;
        cols = matrix[0].length;
        for (int i = 0; i < matrix.length; i++) {
            MapInputFormat t = new MapInputFormat(i, matrix[i]);
            mapInQueue.add(t);
        }

        return this;
    }

    private MatrixNorm map()
    {
        mapOutQueue = new Stack<>();
        while (!mapInQueue.isEmpty()) {

            MapInputFormat in = mapInQueue.pop();
            for (int i = 0; i < in.value.length; i++) {

                mapOutQueue.add(new MapOutputFormat(
                        new Tuple<>(i, false),
                        new Tuple<>(in.key, in.value[i])
                ));

                mapOutQueue.add(new MapOutputFormat(
                        new Tuple<>(i, true),
                        new Tuple<>(in.key, in.value[i])
                ));
            }

        }

        return this;
    }

    private MatrixNorm combine()
    {
        HashMap<Integer, List<Tuple<Integer,Double>>> storage = new HashMap<>();
        while (!mapOutQueue.isEmpty()) {
            MapOutputFormat out = mapOutQueue.pop();

            if(!out.key.value) {
                if (storage.containsKey(out.key.key)) {
                    List<Tuple<Integer, Double>> value = storage.get(out.key.key);
                    value.add(out.value);
                } else {
                    List<Tuple<Integer, Double>> value = new ArrayList<>();
                    value.add(out.value);
                    storage.put(out.key.key, value);
                }
            }
        }

        reduceInQueue = new Stack<>();
        for (int i = 0; i < cols; i++) {
            Tuple<Integer, Boolean> key = new Tuple<>(i, false);
            List<Tuple<Integer,Double>> value = storage.get(i);
            ReduceInputFormat first = new ReduceInputFormat(key, value);
            key = new Tuple<>(i, true);
            ReduceInputFormat second = new ReduceInputFormat(key, value);
            reduceInQueue.add(new ReduceInputPair(first, second));
        }

        return this;
    }

    private MatrixNorm reduce()
    {
        reduceOutQueue = new Stack<>();
        Double[] minValues = new Double[cols];
        Double[] maxValues = new Double[cols];
        while (!reduceInQueue.isEmpty()) {

            ReduceInputPair in = reduceInQueue.pop();

            Double min = Double.MAX_VALUE;
            Double max = Double.MIN_VALUE;

            for (Tuple<Integer, Double> t : in.first.value) {
                if (t.value > max) {
                    max = t.value;
                }
                if (t.value < min) {
                    min = t.value;
                }
            }

            minValues[in.first.key.key] = min;
            maxValues[in.first.key.key] = max;


            for (Tuple<Integer, Double> t : in.second.value) {
                ReduceOutputFormat nt = new ReduceOutputFormat(
                        t.key,
                        new Tuple<>(
                                in.second.key.key,
                                (t.value - minValues[in.second.key.key]) / (maxValues[in.second.key.key] - minValues[in.second.key.key])
                        )
                );

                reduceOutQueue.add(nt);
            }
        }

        return this;
    }

    private double[][] compose()
    {
        double[][] matrixOut = new double[rows][cols];
        while (!reduceOutQueue.isEmpty()) {

            ReduceOutputFormat out = reduceOutQueue.pop();
            int row = out.key;
            int col = out.value.key;
            matrixOut[row][col] = out.value.value;
        }

        return matrixOut;
    }

    public static void main(String[] args) {

        Double[][] matrix = {
                {9., 6.},
                {0., 1.},
                {1., 0.},
                {3., 6.}
        };

        MatrixNorm mn = new MatrixNorm();
        double[][] m = mn.normalize(matrix);

    }
}



