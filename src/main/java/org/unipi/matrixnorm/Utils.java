package org.unipi.matrixnorm;

import java.util.*;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class Utils {

   static int compare(ArrayList<Integer> thisSeq, ArrayList<Integer> thatSeq) throws IllegalArgumentException {

        if (thisSeq.size() != thatSeq.size()) {
            throw new IllegalArgumentException("Arguments have to be of the same length");
        }

        if (0 == thisSeq.get(0).compareTo(thatSeq.get(0))) {

            if (thisSeq.size() > 1) {
                thisSeq.remove(0);
                thatSeq.remove(0);
                return compare(thisSeq, thatSeq);
            }

            return 0;
        }

        return thisSeq.get(0).compareTo(thatSeq.get(0));
    }

    public static String serialize(Double[][] matrix) {

        int rows = matrix.length;
        int cols = matrix[0].length;

        Locale locale  = new Locale("en", "UK");
        String pattern = "#.####";

        DecimalFormat format = (DecimalFormat)
        NumberFormat.getNumberInstance(locale);
        format.applyPattern(pattern);

        StringBuilder b = new StringBuilder();

        b.append(Integer.toString(rows)).append("\t").append(Integer.toString(cols));

        for (Double[] row : matrix) {
            for (int c = 0; c < cols; c++) {
                b.append("\t");
                b.append(format.format(row[c]));
            }
        }

        return b.toString();
    }

    public static Double[][] generateMatrix(int limit, int rows, int cols) {
        Double[][] matrix = new Double[rows][cols];

        for(int r = 0; r < rows; r++) {
            for(int c = 0; c < rows; c++) {
                double leftLimit = 0D;
                double rightLimit = (double) limit;
                matrix[r][c] = leftLimit + new Random().nextDouble() * (rightLimit - leftLimit);
            }
        }

        return matrix;
    }

    private static Double[][] splitArray(Double[] array, int splitSize) {
        List<Double> inputArray = Arrays.asList(array);

        Stream<Integer> startIndicies =
                IntStream.range(0, array.length)
                        .boxed()
                        .collect(Collectors.partitioningBy((Integer i) -> i % splitSize == 0))
                        .get(true)
                        .stream();

        Stream<Stream<Double>> splitArray = startIndicies.map(index -> inputArray.subList(index, index + splitSize).stream());

        return splitArray.map(a -> a.toArray(Double[]::new)).toArray(Double[][]::new);
    }

    public static Double[][] deserialize(String s) throws IllegalArgumentException {
        String[] items = s.split("\t");

        if(items.length < 2)
            throw new IllegalArgumentException();

        int n = Integer.parseInt(items[0]) * Integer.parseInt(items[1]);

        if(n != items.length - 2)
            throw new IllegalArgumentException();

        int cols = Integer.parseInt(items[1]);

        Double[] values = Arrays.asList(items).subList(2, items.length).stream().map(Double::parseDouble).toArray(Double[]::new);

        return Utils.splitArray(values, cols);
    }

    public static void main(String[] args) throws Exception {
        Double[][] matrix = generateMatrix(100, 2, 2);
        System.out.println(serialize(matrix));

        Double[][] matrix1 = deserialize("4\t2\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0\t0.0");
        for(Double[] row: matrix1) {
            for(Double item: row) {
                System.out.print(item);
            }
            System.out.println();
        }

        System.out.println(serialize(matrix1));

        Double[][] matrix2 = deserialize("4\t2\t1\t1\t0\t0.1667\t0.1111\t0\t0.3333\t1");
        for(Double[] row: matrix1) {
            for(Double item: row) {
                System.out.print(item);
            }
            System.out.println();
        }

        System.out.println(serialize(matrix2));

    }
}
