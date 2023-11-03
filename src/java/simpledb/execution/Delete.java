package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private OpIterator child;

    private boolean deleted;

    private boolean opened;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.transactionId = t;
        this.child = child;
        this.deleted = false;
        this.opened = false;
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "countValue" });
    }

    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        super.open();
        this.opened = true;
    }

    public void close() {
        super.close();
        this.child.close();
        this.opened = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        int deleteCount = 0;
        if (deleted) {
            return null;
        }
        if (!opened) {
            throw new DbException("This file is not opened.");
        }
        while (this.child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(this.transactionId, this.child.next());
                deleteCount++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        this.deleted = true;
        Tuple returnTuple = new Tuple(this.getTupleDesc());
        returnTuple.setField(0, new IntField(deleteCount));
        return returnTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
