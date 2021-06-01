package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LockManager {
    private HashMap<TransactionId, Set<Lock>> tidLockMap;
    private HashMap<PageId, Lock> pidLockMap;

    public static final int RUNTIME_LIMIT = 500;
    public static final int WAIT_TIME = 50;

    public LockManager() {
        this.tidLockMap = new HashMap<>();
        this.pidLockMap = new HashMap<>();

    }

    public synchronized void getLock(TransactionId tid, PageId pid, boolean shared) throws TransactionAbortedException {
        Lock lock;
        if (pidLockMap.containsKey(pid)) lock = pidLockMap.get(pid);
        else lock = new Lock(pid, shared);
        if (tidLockMap.containsKey(tid)) tidLockMap.get(tid).add(lock);
        else tidLockMap.put(tid, new HashSet<>(List.of(lock)));
        long startTime = System.currentTimeMillis();
        if (shared) {
            while (lock.getExLockHolders().size() != 0) {
                if (lock.getExLockHolders().contains(tid)) {
                    // already holds the lock
                    lock.getExCandidates().add(tid);
                    lock.setShared(true);
                    break;
                }
                if (System.currentTimeMillis() - startTime > RUNTIME_LIMIT) {
                    throw new TransactionAbortedException();
                }
                try {
                    wait(WAIT_TIME);
                } catch (Exception e) {
                    throw new TransactionAbortedException();
                }
            }
            lock.getSharedLockHolders().add(tid);
            this.pidLockMap.put(pid, lock);
        } else {
            // Exclusive Lock
            while (lock.getSharedLockHolders().size() != 0 || lock.getExLockHolders().size() != 0) {
                if (lock.getExLockHolders().contains(tid)) break;
                else if (lock.getExLockHolders().isEmpty()) {
                    if (lock.getSharedLockHolders().contains(tid) && lock.getSharedLockHolders().size() == 1) {
                        lock.getSharedCandidates().add(tid);
                        break;
                    }
                }
                if (System.currentTimeMillis() - startTime > RUNTIME_LIMIT) {
                    throw new TransactionAbortedException();
                }
                try {
                    wait(WAIT_TIME);
                } catch (Exception e) {
                    throw new TransactionAbortedException();
                }
            }
            lock.getExLockHolders().add(tid);
            lock.setShared(false);
            this.pidLockMap.put(pid, lock);
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (tidLockMap.containsKey(tid) && pidLockMap.containsKey(pid)) {
            Lock releasedLock = pidLockMap.get(pid);
            if (releasedLock.getExLockHolders().contains(tid)) {
                releasedLock.getExLockHolders().remove(tid);
                releasedLock.getSharedLockHolders().remove(tid);
                this.tidLockMap.get(tid).remove(releasedLock);
                this.pidLockMap.remove(pid);
            } else if (releasedLock.getSharedLockHolders().contains(tid)) {
                releasedLock.getSharedLockHolders().remove(tid);
                if (releasedLock.getSharedLockHolders().isEmpty() && releasedLock.getExLockHolders().isEmpty()) {
                    this.tidLockMap.get(tid).remove(releasedLock);
                    this.pidLockMap.remove(pid);
                }
            }
        }
        notifyAll();
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        if (this.tidLockMap.containsKey(tid)) {
            // get All lock pids
            getExLockedPids(tid).forEach(pid -> {
                releaseLock(tid, pid);
            });
            getShareLockedPids(tid).forEach(pid -> {
                releaseLock(tid, pid);
            });
        }
        this.tidLockMap.remove(tid);
        notifyAll();
    }

    public synchronized HashSet<PageId> getExLockedPids(TransactionId tid) {
        HashSet<PageId> ans = new HashSet<>();
        if (tidLockMap.containsKey(tid)) {
            for (Lock lock : tidLockMap.get(tid)) {
                if (lock.getExLockHolders().contains(tid) || lock.getExCandidates().contains(tid)) {
                    ans.add(lock.pid);
                }
            }
        }
        return ans;
    }

    public synchronized HashSet<PageId> getShareLockedPids(TransactionId tid) {
        HashSet<PageId> ans =  new HashSet<>();
        if (tidLockMap.containsKey(tid)) {
            for (Lock lock : tidLockMap.get(tid)) {
                if (lock.getSharedLockHolders().contains(tid) || lock.getSharedCandidates().contains(tid)) {
                    ans.add(lock.pid);
                }
            }
        }
        return ans;
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        if (tidLockMap.containsKey(tid)) {
            for (Lock lock : tidLockMap.get(tid)) {
                if (lock.pid == pid) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean holdsOneLock(TransactionId tid) {
        return tidLockMap.containsKey(tid);
    }

}
