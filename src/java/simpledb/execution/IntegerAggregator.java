package simpledb.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByFieldIndex;
    private Type groupByFieldType;
    private int aggregateFieldIndex;
    private Op operator;

    private HashMap<Field, ArrayList<Integer>> groupedFieldValues;
    private ArrayList<Integer> noGroupingFieldValues;

    /**
     * Aggregate constructor
     *
     * @param gbf  the 0-based index of the group-by field in the tuple, or
     *             NO_GROUPING if there is no grouping
     * @param gbft the type of the group by field (e.g., Type.INT_TYPE), or
     *             null
     *             if there is no grouping
     * @param af   the 0-based index of the aggregate field in the tuple
     * @param op   the aggregation operator
     */

    public IntegerAggregator(int gbf, Type gbft, int af, Op op) {
        this.groupByFieldIndex = gbf;
        this.groupByFieldType = gbft;
        this.aggregateFieldIndex = af;
        this.operator = op;
        if (this.groupByFieldIndex == NO_GROUPING) {
            this.noGroupingFieldValues = new ArrayList<>();
        } else {
            this.groupedFieldValues = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField aggregateField = (IntField) tup.getField(this.aggregateFieldIndex);
        Integer aggregateFieldValue = aggregateField.getValue();
        if (this.groupByFieldIndex == NO_GROUPING) {
            this.noGroupingFieldValues.add(aggregateFieldValue);
        } else {
            Field groupByField = tup.getField(this.groupByFieldIndex);
            if (this.groupedFieldValues.get(groupByField) == null) {
                this.groupedFieldValues.put(groupByField, new ArrayList<>());
            }
            this.groupedFieldValues.get(groupByField).add(aggregateFieldValue);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();

        if (this.groupByFieldIndex == NO_GROUPING) {
            int aggregateValue = IntegerAggregator.aggregate(this.noGroupingFieldValues, this.operator);

            TupleDesc tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "aggregateValue" });
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(aggregateValue));
            tuples.add(tuple);
            return new TupleIterator(tupleDesc, tuples);
        }

        TupleDesc tupleDesc = new TupleDesc(new Type[] { this.groupByFieldType, Type.INT_TYPE },
                new String[] { "groupValue", "aggregateValue" });
        for (Field f : this.groupedFieldValues.keySet()) {
            ArrayList<Integer> groupValues = this.groupedFieldValues.get(f);

            int aggregateValue = IntegerAggregator.aggregate(groupValues, this.operator);
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, f);
            tuple.setField(1, new IntField(aggregateValue));
            tuples.add(tuple);
        }
        return new TupleIterator(tupleDesc, tuples);
    }

    private static int aggregate(ArrayList<Integer> values, Op operator) {
        int aggregateValue = 0;
        switch (operator) {
            case MAX:
                aggregateValue = Collections.max(values);
                break;
            case MIN:
                aggregateValue = Collections.min(values);
                break;
            case SUM:
                aggregateValue = values.stream().mapToInt(Integer::intValue).sum();
                break;
            case AVG:
                aggregateValue = (int) values.stream().mapToInt(Integer::intValue).average()
                        .getAsDouble();
                break;
            case COUNT:
                aggregateValue = values.size();
                break;
            default:
                break;
        }
        return aggregateValue;
    }
}
