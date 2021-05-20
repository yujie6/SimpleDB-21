package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private HeapFile heapFile;
    private HeapPage heapPage;
    private Iterator<Tuple> iterator;
    private TransactionId tid;
    private int curPageId;
    private boolean closed;
    private Tuple next;
    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.heapFile = hf;
        this.tid = tid;
        this.curPageId = 0;
        closed = true;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        closed = false;
        curPageId = 0;
        HeapPageId pid = new HeapPageId(heapFile.getId(), curPageId);
        try {
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }
        iterator = heapPage.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (closed)
            return false;

        if (next == null)
            next = fetchNext();
        return next != null;
    }

    public Tuple fetchNext() throws DbException {
        if (closed) {
            return null;
        }
        if (iterator.hasNext()) {
            return iterator.next();
        }
        while (true) {
            curPageId++;
            if (curPageId >= heapFile.numPages()) {
                return null;
            }
            HeapPageId pid = new HeapPageId(heapFile.getId(), curPageId);
            try {
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            } catch (TransactionAbortedException | DbException e) {
                e.printStackTrace();
            }
            iterator = heapPage.iterator();
            if (iterator.hasNext()) {
                return iterator.next();
            }
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (closed) throw new NoSuchElementException();
        if (next == null) {
            next = fetchNext();
            if (next == null) {
                throw new NoSuchElementException();
            }
        }

        Tuple result = next;
        next = null;
        return result;

    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        open();
    }

    @Override
    public void close() {
        closed = true;
    }
}
