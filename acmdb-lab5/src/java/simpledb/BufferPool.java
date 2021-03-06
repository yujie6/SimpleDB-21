package simpledb;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private HashMap<PageId, Page> bufferContents;
    private HashMap<PageId, Integer> pageUseTime;
    private int maxPageNum;
    private LockManager lockManager;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        bufferContents = new HashMap<>();
        pageUseTime = new HashMap<>();
        maxPageNum = numPages;
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if (perm == Permissions.READ_WRITE) {
            lockManager.getLock(tid, pid, false);
        } else if (perm == Permissions.READ_ONLY) {
            lockManager.getLock(tid, pid, true);
        }

        synchronized (this) {
            if (bufferContents.containsKey(pid)) {
                if (!pageUseTime.containsKey(pid)) pageUseTime.put(pid, 1);
                else pageUseTime.replace(pid, pageUseTime.get(pid) + 1);
                return bufferContents.get(pid);
            } else {
                if (bufferContents.size() == maxPageNum) {
                    try {
                        evictPage();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                DbFile hf = Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page newPage = hf.readPage(pid);
                bufferContents.put(pid, newPage);
                pageUseTime.put(pid, 1);
                return newPage;
            }
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        if (commit) {
            this.flushPages(tid);
        } else {
            if (lockManager.holdsOneLock(tid)) {
                lockManager.getExLockedPids(tid).forEach(this::discardPage);
            }
        }

        if (!lockManager.holdsOneLock(tid)) {
            notifyAll();
            return;
        }
        lockManager.releaseAllLocks(tid);

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile bTreeFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = bTreeFile.insertTuple(tid, t);
        synchronized (this) {
            for (Page page : dirtyPages) {
                page.markDirty(true, tid);
                if (!bufferContents.containsKey(page.getId())) {
                    bufferContents.put(page.getId(), page);
                } else bufferContents.replace(page.getId(), page);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile bTreeFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = bTreeFile.deleteTuple(tid, t);
        synchronized (this) {
            for (Page page : dirtyPages) {
                page.markDirty(true, tid);
                bufferContents.replace(page.getId(), page);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : bufferContents.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        if (bufferContents.containsKey(pid)) {
            bufferContents.remove(pid);
            pageUseTime.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (bufferContents.containsKey(pid)) {
            Page dirtyPage = bufferContents.get(pid);
            TransactionId lastTransaction = dirtyPage.isDirty();
            if (lastTransaction != null) {
                dirtyPage.markDirty(false, lastTransaction);
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(dirtyPage);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        if (lockManager.holdsOneLock(tid)) lockManager.getExLockedPids(tid).forEach(pid -> {
            try {
                flushPage(pid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private PageId getLeastUsedPID() {
        int minVal = 1000000;
        PageId target = null;
        for (PageId pid : pageUseTime.keySet()) {
            if (bufferContents.get(pid).isDirty() == null) {
                if (pageUseTime.get(pid) < minVal) {
                    minVal = pageUseTime.get(pid);
                    target = pid;
                }
            }
        }
        return target;
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException, IOException {
        // Use LRU
        // update: cannot evict dirty page!
        PageId removeID = getLeastUsedPID();
        if (removeID == null) {
            throw new DbException("All pages are dirty, cannot evict!");
        }
        flushPage(removeID);
        bufferContents.remove(removeID);
        pageUseTime.remove(removeID);
    }

}
