package org.unipi.matrixnorm;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.ArrayList;
import java.util.stream.*;


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

    public static Double[][] splitArray(Double[] array, int splitSize) {
        List<Double> inputArray = Arrays.asList(array);

        Stream<Integer> startIndicies =
                IntStream.range(0, array.length)
                        .boxed()
                        .collect(Collectors.partitioningBy((Integer i) -> i % splitSize == 0))
                        .get(true)
                        .stream();

        Stream<Stream<Double>> splitArray = startIndicies.map(index -> inputArray.subList(index, index + splitSize).stream());

        return splitArray.map(a -> a.toArray(Integer[]::new)).toArray(Double[][]::new);
    }

    public static <T> List<T> twoDArrayToList(T[][] twoDArray) {
        List<T> list = new ArrayList<T>();
        for (T[] array : twoDArray) {
            list.addAll(Arrays.asList(array));
        }

        return list;
    }

    public static List<List<Double>> twoDArrayToList(Double[][] twoDArray) {
        ArrayList<List<Double>> matrix = new ArrayList<>();
        for (Double[] array : twoDArray) {
            matrix.add(Arrays.asList(array));
        }

        return matrix;
    }

    public static String serialize(Double[][] matrix) {
        return "";
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
