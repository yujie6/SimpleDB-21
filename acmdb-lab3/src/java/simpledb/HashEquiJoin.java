package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate jp;
    private DbIterator child1, child2;
    private TupleDesc mergedTD;
    private Tuple t1, t2;
    private ArrayList<Tuple> joinedArray;
    private Iterator<Tuple> joinedIterator;
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.jp = p;
        this.child1 = child1;
        this.child2 = child2;
        this.mergedTD = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        return jp;
    }

    public TupleDesc getTupleDesc() {
        return mergedTD;
    }

    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(jp.getField1());
    }

    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(jp.getField2());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
        joinedArray = new ArrayList<>();
        ArrayList<Tuple> tmp = new ArrayList<>();
        while (child1.hasNext()) {
            tmp.add(child1.next());
        }
        while (child2.hasNext()) {
            Tuple t2 = child2.next();
            for (Tuple t : tmp) {
                if (jp.filter(t, t2)) {
                    joinedArray.add(Tuple.merge(mergedTD, t, t2));
                }
            }
        }
        joinedIterator = joinedArray.iterator();
    }

    public void close() {
        this.child1.close();
        this.child2.close();

        super.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        joinedIterator = joinedArray.iterator();
        t1 = null;
        t2 = null;
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (joinedIterator.hasNext()) {
            return joinedIterator.next();
        }
        return null;
        /*if (t1 == null && t2 == null) {
            t1 = child1.next();
            t2 = child2.next();
            if (jp.filter(t1, t2)) return Tuple.merge(mergedTD, t1, t2);
        }
        while (true) {
            if (child2.hasNext()) {
                t2 = child2.next();
                if (jp.filter(t1, t2)) return Tuple.merge(mergedTD, t1, t2);
            } else {
                if (child1.hasNext()) {
                    t1 = child1.next();
                    child2.rewind();
                    t2 = child2.next();
                    if (jp.filter(t1, t2)) return Tuple.merge(mergedTD, t1, t2);
                } else return null;
            }
        }*/
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] children = new DbIterator[2];
        children[0] = child1;
        children[1] = child2;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
