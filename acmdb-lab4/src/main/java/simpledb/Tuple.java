package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private ArrayList<Field> tupleContents;
    private TupleDesc td;
    private RecordId rid;

    public Tuple(TupleDesc td) {
        tupleContents = new ArrayList<>();
        tupleContents.addAll(Arrays.asList(new Field[td.numFields()]));
        this.td = td;
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public RecordId getRecordId() {
        return rid;
    }

    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    public void setField(int i, Field f) {
        tupleContents.set(i, f);
    }

    public Field getField(int i) {
        if (i >= tupleContents.size() || i < 0) return null;
        return tupleContents.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Field field : tupleContents) {
            sb.append(field.toString()).append("\t");
        }
        return sb.toString();
    }

    public Iterator<Field> fields() {
        return tupleContents.iterator();
    }

    public static Tuple merge(TupleDesc td, Tuple t1, Tuple t2) {
        Tuple merged = new Tuple(td);
        merged.tupleContents.clear();
        merged.tupleContents.addAll(t1.tupleContents);
        merged.tupleContents.addAll(t2.tupleContents);
        return merged;
    }

    public void resetTupleDesc(TupleDesc td) {
        this.td = td;
    }
}
