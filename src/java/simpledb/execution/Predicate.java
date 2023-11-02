package simpledb.execution;

import java.io.Serializable;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constants used for return codes in Field.compare
     */

    private final int fieldNum;

    private final Op operator;

    private final Field operand;

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

    /**
     * Constructor.
     *
     * @param fieldNum field number of passed in tuples to compare against.
     * @param operator operation to use for comparison
     * @param operand  field value to compare passed in tuples to
     */
    public Predicate(int fieldNum, Op operator, Field operand) {
        this.fieldNum = fieldNum;
        this.operator = operator;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField() {
        return this.fieldNum;
    }

    /**
     * @return the operator
     */
    public Op getOp() {
        return this.operator;
    }

    /**
     * @return the operand
     */
    public Field getOperand() {
        return this.operand;
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
        return t.getField(this.getField()).compare(this.getOp(), this.getOperand());
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    @Override
    public String toString() {
        return String.format("fieldId: %s, operator: %s, operand: %s", this.fieldNum, this.operator, this.operand);
    }
}
