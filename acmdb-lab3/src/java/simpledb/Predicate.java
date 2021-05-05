package simpledb;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constants used for return codes in Field.compare
     */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    private int fieldNum;
    private Op op;
    private Field comparedValue;

    public Predicate(int field, Op op, Field operand) {
        this.fieldNum = field;
        this.op = op;
        this.comparedValue = operand;
    }

    public int getField() {
        return fieldNum;
    }

    public Op getOp() {
        return op;
    }

    public Field getOperand() {
        return comparedValue;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        return t.getField(fieldNum).compare(op, getOperand());
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string
     */
    public String toString() {
        StringBuilder sb = new StringBuilder("f=");
        sb.append(fieldNum).append(", op=").append(op.toString()).append(", operand=").append(getOperand());
        return sb.toString();
    }
}
