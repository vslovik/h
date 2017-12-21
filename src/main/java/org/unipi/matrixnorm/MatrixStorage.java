package org.unipi.matrixnorm;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class MatrixStorage {

    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;

    private TreeMap<Integer, Double[]> treeMap = new TreeMap<>();

    void keepMinMax(Iterable<MapperValue> values) {
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
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

        int cols = treeMap.entrySet().size();
        int rows = treeMap.firstEntry().getValue().length;

        Double[][] matrix = new Double[rows][cols];
        for(Map.Entry<Integer, Double[]> entry : treeMap.entrySet()) {
            int c = entry.getKey();
            Double[] values = entry.getValue();
            for(int r = 0; r < rows; r++) {
                matrix[r][c] = values[r];
            }
        }

        return matrix;
    }

    void put(int colIndex, Iterable<MapperValue> values) {
        treeMap.put(colIndex, keepColumn(values));
    }

    Double[][] get() {
        Double[][] matrix = treeMapTo2DArray();
        treeMap = new TreeMap<>();
        return matrix;
    }

    public static void main(String[] args) throws Exception {

        MatrixStorage storage = new MatrixStorage();

        ArrayList<MapperValue> values = new ArrayList<>();
        values.add(new MapperValue(0, 0, 0.0));
        values.add(new MapperValue(0, 1, 0.0));
        values.add(new MapperValue(0, 2, 0.0));
        values.add(new MapperValue(0, 3, 0.0));

        storage.keepMinMax(values);
        storage.put(0, values);

        storage.keepMinMax(values);
        storage.put(1, values);

        System.out.println(Utils.serialize(storage.get()));

        ArrayList<MapperValue> values1 = new ArrayList<>();
        values1.add(new MapperValue(0, 0, 9.0));
        values1.add(new MapperValue(0, 1, 0.0));
        values1.add(new MapperValue(0, 2, 1.0));
        values1.add(new MapperValue(0, 3, 3.0));

        storage.keepMinMax(values1);
        storage.put(0, values1);

        ArrayList<MapperValue> values2 = new ArrayList<>();
        values2.add(new MapperValue(0, 0, 6.0));
        values2.add(new MapperValue(0, 1, 1.0));
        values2.add(new MapperValue(0, 2, 0.0));
        values2.add(new MapperValue(0, 3, 6.0));

        storage.keepMinMax(values2);
        storage.put(1, values2);

        System.out.println(Utils.serialize(storage.get()));
    }

}
