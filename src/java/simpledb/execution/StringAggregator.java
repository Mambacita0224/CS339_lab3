package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByFieldIndex;
    private Type groupByFieldType;

    private HashMap<Field, Integer> groupCount;
    private int noGroupingCount;

    /**
     * Aggregate constructor
     *
     * @param gbf  the 0-based index of the group-by field in the tuple, or
     *             NO_GROUPING if there is no grouping
     * @param gbft the type of the group by field (e.g., Type.INT_TYPE), or
     *             null if there is no grouping
     * @param af   the 0-based index of the aggregate field in the tuple
     * @param op   aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if op != COUNT
     */

    public StringAggregator(int gbf, Type gbft, int af, Op op) throws IllegalArgumentException {
        if (op != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.groupByFieldIndex = gbf;
        this.groupByFieldType = gbft;
        if (this.groupByFieldIndex == NO_GROUPING) {
            this.noGroupingCount = 0;
        } else {
            this.groupCount = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (this.groupByFieldIndex == NO_GROUPING) {
            this.noGroupingCount++;
        } else {
            Field groupByField = tup.getField(this.groupByFieldIndex);
            if (this.groupCount.get(groupByField) == null) {
                this.groupCount.put(groupByField, 0);
            }
            this.groupCount.put(groupByField, this.groupCount.get(groupByField) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();

        if (this.groupByFieldIndex == NO_GROUPING) {
            TupleDesc tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "aggregateValue" });
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(this.noGroupingCount));
            tuples.add(tuple);
            return new TupleIterator(tupleDesc, tuples);
        }

        TupleDesc tupleDesc = new TupleDesc(new Type[] { this.groupByFieldType, Type.INT_TYPE },
                new String[] { "groupValue", "aggregateValue" });
        for (Field f : this.groupCount.keySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, f);
            tuple.setField(1, new IntField(this.groupCount.get(f)));
            tuples.add(tuple);
        }
        return new TupleIterator(tupleDesc, tuples);
    }
}
