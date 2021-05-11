package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbField, aField;
    private Type gbFieldType;
    private Op what;
    private String gbFieldName;
    private ArrayList<Tuple> aggResult;
    private HashMap<Field, ArrayList<Field>> aggMap;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.what = what;
        this.aField = afield;
        this.aggResult = new ArrayList<>();
        this.aggMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gVal = tup.getField(gbField);
        gbFieldName = tup.getTupleDesc().getFieldName(gbField);
        if (!aggMap.containsKey(gVal)) {
            aggMap.put(gVal, new ArrayList<>(List.of(tup.getField(aField))));
        } else {
            aggMap.get(gVal).add(tup.getField(aField));
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        switch (what) {
            case COUNT: {
                TupleDesc td = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE},
                        new String[]{gbFieldName, "CountResult"});
                aggMap.forEach((gval, gList) -> {
                    IntField aggValue = new IntField(gList.size());
                    Tuple aggTuple = new Tuple(td);
                    aggTuple.setField(0, gval);
                    aggTuple.setField(1, aggValue);
                    aggResult.add(aggTuple);
                });
                return new TupleIterator(td, aggResult);
            }
            default: {
                System.err.print("Not implemented agg op");
                return null;
            }
        }
    }

}
