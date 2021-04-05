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
    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.heapFile = hf;
        this.tid = tid;
        this.curPageId = 0;
        HeapPageId pid = new HeapPageId(hf.getId(), curPageId);
        try {
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }
        iterator = heapPage.iterator();
        closed = true;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        closed = false;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (closed) return false;
        if (iterator.hasNext()) return true;
        else return curPageId < heapFile.numPages() - 1;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (closed) {
            throw new NoSuchElementException();
        }
        if (iterator.hasNext()) return iterator.next();
        else {
            curPageId++;
            HeapPageId pid = new HeapPageId(heapFile.getId(), curPageId);
            try {
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            } catch (TransactionAbortedException | DbException e) {
                e.printStackTrace();
            }
            iterator = heapPage.iterator();
            return iterator.next();
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        HeapPageId pid = new HeapPageId(heapFile.getId(), 0);
        try {
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }
        iterator = heapPage.iterator();
    }

    @Override
    public void close() {
        closed = true;
    }
}
