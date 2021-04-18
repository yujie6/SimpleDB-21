package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        public final Type fieldType;

        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType &&
                    Objects.equals(fieldName, tdItem.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldType, fieldName);
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> tdContents;

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdContents.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        tdContents = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            tdContents.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    public TupleDesc(TupleDesc other, String prefix) {
        tdContents = new ArrayList<>();
        for (int i = 0; i < other.numFields(); i++) {
            tdContents.add(new TDItem(other.getFieldType(i), prefix + "." + other.getFieldName(i)));
        }
    }

    public TupleDesc(ArrayList<TDItem> itemList) {
        tdContents = new ArrayList<>();
        tdContents.addAll(itemList);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tdContents = new ArrayList<>();
        for (Type type : typeAr) {
            tdContents.add(new TDItem(type, ""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdContents.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= numFields() || i < 0) throw new NoSuchElementException();
        else return tdContents.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= numFields() || i < 0) throw new NoSuchElementException();
        else return tdContents.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < numFields(); i++) {
            if (tdContents.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : tdContents) {
            size += item.fieldType.getLen();
        }
        return size;
    }


    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        ArrayList<TDItem> itemList = new ArrayList<>(td1.tdContents);
        itemList.addAll(td2.tdContents);
        return new TupleDesc(itemList);
    }

    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (o instanceof TupleDesc) {
            if (((TupleDesc) o).numFields() == numFields()) {
                for (int i = 0; i < numFields(); i++) {
                    if (!tdContents.get(i)
                            .equals(((TupleDesc) o).tdContents.get(i)))
                        return false;
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return tdContents.hashCode();
    }

    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields(); i++) {
            sb.append(getFieldType(i).toString()).append("(").append(getFieldName(i)).append(")");
        }
        return sb.toString();
    }
}
