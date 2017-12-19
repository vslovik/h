package org.unipi.matrixnorm;

import java.util.ArrayList;
import java.util.Arrays;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Utils {

   public static int compare(ArrayList<Integer> thisSeq, ArrayList<Integer> thatSeq) throws IllegalArgumentException {

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

        return String.join("\t", Integer.toString(matrix.length), Integer.toString(matrix[0].length), toString(matrix));
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

    private static String toString(Double[][] a) {

        if (a == null)
            return "";

        int iMax = a.length - 1;
        if (iMax == -1)
            return "";

        DecimalFormat format = new DecimalFormat("#.####");

        StringBuilder b = new StringBuilder();
        for (int i = 0; ; i++) {
            b.append(format.format(a[i]));
            if (i == iMax)
                return b.toString();
            b.append("\t");
        }
    }

    public static Double[][] splitArray(Double[] array, int splitSize) {
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

        int rows = Integer.parseInt(items[0]);
        int cols = Integer.parseInt(items[1]);

        Double[] values = Arrays.asList(items).subList(2, items.length).stream().map(Double::parseDouble).toArray(Double[]::new);

        return Utils.splitArray(values, cols);
    }
}
