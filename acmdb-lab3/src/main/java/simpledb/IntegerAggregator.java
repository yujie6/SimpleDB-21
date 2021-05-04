package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     * the 0-based index of the group-by field in the tuple, or
     * NO_GROUPING if there is no grouping
     * @param gbfieldtype
     * the type of the group by field (e.g., Type.INT_TYPE), or null
     * if there is no grouping
     * @param afield
     * the 0-based index of the aggregate field in the tuple
     * @param what
     * the aggregation operator
     */
    private int gbField, aField;
    private Type gbFieldType;
    private Op what;
    private ArrayList<Tuple> aggResult;
    private HashMap<Field, ArrayList<Field>> aggMap;
    private String gbFieldName;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.what = what;
        this.aField = afield;
        this.aggResult = new ArrayList<>();
        this.aggMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
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
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        aggResult.clear();

        TupleDesc td = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE},
                new String[]{gbFieldName, "AggregateResult"});
        switch (what) {
            case MAX: {
                aggMap.forEach((gval, gList) -> {
                    int ans = (int) -1e8;
                    for (Field i : gList) {
                        ans = Math.max(ans, ((IntField) i).getValue());
                    }
                    Tuple aggTuple = new Tuple(td);
                    aggTuple.setField(0, gval);
                    aggTuple.setField(1, new IntField(ans));
                    aggResult.add(aggTuple);
                });
                break;
            }
            case MIN: {
                aggMap.forEach((gval, gList) -> {
                    int ans = (int) 1e8;
                    for (Field i : gList) {
                        ans = Math.min(ans, ((IntField) i).getValue());
                    }
                    Tuple aggTuple = new Tuple(td);
                    aggTuple.setField(0, gval);
                    aggTuple.setField(1, new IntField(ans));
                    aggResult.add(aggTuple);
                });
                break;
            }
            case SUM: {
                aggMap.forEach((gval, gList) -> {
                    int sum = 0;
                    for (Field i : gList) {
                        sum += ((IntField) i).getValue();
                    }
                    Tuple aggTuple = new Tuple(td);
                    aggTuple.setField(0, gval);
                    aggTuple.setField(1, new IntField(sum));
                    aggResult.add(aggTuple);
                });
                break;
            }
            case COUNT: {
                aggMap.forEach((gval, gList) -> {
                    IntField aggValue = new IntField(gList.size());
                    Tuple aggTuple = new Tuple(td);
                    aggTuple.setField(0, gval);
                    aggTuple.setField(1, aggValue);
                    aggResult.add(aggTuple);
                });
                break;
            }
            case SUM_COUNT: {
                break;
            }
            case AVG: {
                aggMap.forEach((gval, gList) -> {
                    int sum = 0;
                    for (Field i : gList) {
                        sum += ((IntField) i).getValue();
                    }
                    Tuple aggTuple = new Tuple(td);
                    aggTuple.setField(0, gval);
                    aggTuple.setField(1, new IntField(sum / gList.size()));
                    aggResult.add(aggTuple);
                });
            }
            case SC_AVG: {
                break;
            }
        }
        return new TupleIterator(td, aggResult);
    }

}
