package org.unipi.matrixnorm;

public class Tuple<Key, Value> {
    public final Key key;
    public final Value value;
    public Tuple(Key key, Value value) {
        this.key = key;
        this.value = value;
    }
}