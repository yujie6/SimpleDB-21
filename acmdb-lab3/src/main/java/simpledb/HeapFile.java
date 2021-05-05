package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {


    private File f;
    private TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            if (pid.pageNumber() >= numPages()) throw new DbException("Read page invalid!");
            FileInputStream in = new FileInputStream(f);
            in.skip(pid.pageNumber() * BufferPool.getPageSize());
            byte[] data = new byte[BufferPool.getPageSize() + 23];
            int success = in.readNBytes(data, 0, BufferPool.getPageSize());
            if (success == -1) System.out.println("Reaching the end of file!!");
            in.close();
            return new HeapPage(((HeapPageId) pid), data);
        } catch (IOException | DbException e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        byte[] pageData = page.getPageData();
        RandomAccessFile randomAccessFile = new RandomAccessFile(f, "rw");
        randomAccessFile.seek(page.getId().pageNumber() * BufferPool.getPageSize());
        randomAccessFile.write(pageData, 0, BufferPool.getPageSize());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(f.length() / (BufferPool.getPageSize() + 0.0));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> dirtyPages = new ArrayList<Page>();
        int pageNum = numPages();
        for (int i = 0; i < pageNum; i++) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage tmpPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            try {
                tmpPage.insertTuple(t);
                dirtyPages.add(tmpPage);
                return dirtyPages;
            } catch (DbException e) {
            }
        }
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), pageNum), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        dirtyPages.add(newPage);
        writePage(newPage);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> dirtyPages = new ArrayList<Page>();
        HeapPage targetPage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        targetPage.deleteTuple(t);
        dirtyPages.add(targetPage);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

