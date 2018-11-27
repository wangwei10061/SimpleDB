package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** 
     * 
     */
    private volatile LockManager lockManager;
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** param for buffer capacity numPages*/
    private volatile int numPages;
    /** structure to store buffer pages*/
    private volatile Map<PageId, Page> bufferMap;//pageId, page

	private volatile Map<PageId, Integer> recentlyUsed;
	
	private volatile Map<TransactionId, Long> allTransactions;


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
        // some code goes here
        this.numPages=numPages;
        bufferMap=new ConcurrentHashMap<PageId, Page>();
	    recentlyUsed=new ConcurrentHashMap<PageId, Integer>();
	allTransactions = new ConcurrentHashMap<TransactionId, Long>();
        lockManager = new LockManager();

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
        throws TransactionAbortedException, DbException{
        //check whether to grant lock for this transaction

if (!allTransactions.containsKey(tid)) {//if this is new transaction
        long t0 = System.currentTimeMillis();
	allTransactions.put(tid, t0);
	
        boolean notGranted = lockManager.grantLock(pid, tid, perm);
        //put on queue if the lock is not granted
        while( notGranted){
            //long t1 = System.currentTimeMillis();
            //Rex: you can tweak these numbers, originally kept requesting not working/slow
            if ((System.currentTimeMillis() - allTransactions.get(tid)) > 250) {
                //lockManager.releaseAllTidLocks(tid);
                throw new TransactionAbortedException();
            }
            try {
                Thread.sleep(200);
                notGranted = lockManager.grantLock(pid, tid, perm);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }//ends while
} else {//is an already running transsaction

        boolean notGranted = lockManager.grantLock(pid, tid, perm);
        //put on queue if the lock is not granted
        while( notGranted){
            //long t1 = System.currentTimeMillis();
            //Rex: you can tweak these numbers, originally kept requesting not working/slow
            if ((System.currentTimeMillis() - allTransactions.get(tid)) > 500) {
                //lockManager.releaseAllTidLocks(tid);
                throw new TransactionAbortedException();
            }
            try {
                Thread.sleep(10);
                notGranted = lockManager.grantLock(pid, tid, perm);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }//ends while
}


        // some code goes here
        //check if the page is in the bufferMap

        if (bufferMap.containsKey(pid)){
            //update the recentlyUsed hashmap
            updateRecentlyUsed();
		  recentlyUsed.put(pid, 0);
          //this page was just accessed
        return bufferMap.get(pid);

        } else {
            List<Table>tableList=Database.getCatalog().getTables();
            for (Table t: tableList){
                if (t.get_file().getId()==pid.getTableId()){
                    DbFile file=t.get_file();
                    Page pageRead=file.readPage(pid);
                    if (numPages<=bufferMap.size()){
			//will have to deal with eviction here
			evictPage();
                    }
                    bufferMap.put(pid,pageRead);
			updateRecentlyUsed();
			recentlyUsed.put(pid, 0);//this page was just accessed
                    return pageRead;  
                }           
            }

        }      
            throw new DbException("page requested not in bufferpool or disk");
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
	//commit: flush transaction pages
	//abort: revert changes
	//release BufferPool states related to transaction, release locks

	allTransactions.remove(tid);
	if (commit == true) {
        for (Page page : bufferMap.values()) {
            if (page.isDirty()!=null && page.isDirty().equals(tid)) {
                flushPages(tid);
                page.setBeforeImage();
            }

            if (page.isDirty() == null){
                page.setBeforeImage();
            }
        }
	} else {
		for (Page page : bufferMap.values()) {
			if (page.isDirty()!=null && page.isDirty().equals(tid)) {
				bufferMap.put(page.getId(), page.getBeforeImage());
			}
		}
	}
	//release all locks related to transaction
	lockManager.releaseAllTidLocks(tid);


    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        try{
        	ArrayList<Page> affectedPages;
        	DbFile dbFile = Database.getCatalog().getDbFile(tableId);
        	HeapFile heapFile = (HeapFile)dbFile;
        	affectedPages = heapFile.insertTuple(tid, t);
        	//iterate through affectedPages and markDirty
		//also update cached pages
                
            int size = affectedPages.size();
        	for (Page page : affectedPages) {
    		page.markDirty(true,tid);
			bufferMap.put(page.getId(), page);
        	}
        }
        catch (DbException e){
                e.printStackTrace();
            }
        catch (TransactionAbortedException e){
                e.printStackTrace();
            }
        catch (IOException e){
                e.printStackTrace();
            }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        try
        {
    	int tableId = t.getRecordId().getPageId().getTableId(); 
    	DbFile dbFile = Database.getCatalog().getDbFile(tableId);
    	HeapFile heapFile = (HeapFile)dbFile;
    	Page affectedPage = heapFile.deleteTuple(tid, t);
    	//iterate through affectedPages and markDirty
        affectedPage.markDirty(true,tid);
        }
        catch (DbException e){
                e.printStackTrace();
            }
        catch (TransactionAbortedException e){
                e.printStackTrace();
            }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
	//call flushPage on all pages in the bp
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
    	for (PageId key : bufferMap.keySet()) {
    		flushPage(key);
    	}

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
    // not necessary for proj1
        bufferMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    //write page to dsk and mark as not dirty
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
    	Page page = bufferMap.get(pid);
    	int tableId = ((HeapPageId)pid).getTableId();
    	HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(tableId);
    	hf.writePage(page);
    	page.markDirty(false, null);
	
    }

    /** Write all pages of the specified transaction to disk.
	* NEED FOR PROJ2?????
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        for (Page page : bufferMap.values()) {
        	if (page.isDirty() !=null && page.isDirty()==tid) {
        		flushPage(page.getId());
        	}
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    	Page evictedPage;
    	int counter = -1;
    	PageId evictedPageId = null;
        boolean isPageDirty = true;
        int dirtyPageCount = 0;
      
        for (PageId key : bufferMap.keySet()) {
            isPageDirty = ((HeapPage)bufferMap.get(key)).dirty;
            if (isPageDirty){
                dirtyPageCount++;
            }
        }
        //if all pages are dirty
        if (dirtyPageCount == numPages){
            throw new DbException("all pages in BufferPool are dirty.");
        }
        isPageDirty = true;
        //Check to make sure that the page evicted is not a dirty page
    	for (PageId key : bufferMap.keySet()) {
    		int value = recentlyUsed.get(key);
    		if (value > counter) {	
    			counter = value;
    			evictedPageId = key;
                evictedPage = bufferMap.get(evictedPageId);
                isPageDirty = ((HeapPage)evictedPage).dirty;
                if (!isPageDirty){
                    try{
                        flushPage(evictedPageId);
                        bufferMap.remove(evictedPageId);
                        recentlyUsed.remove(evictedPageId);
                        break;
                        
                        }catch (IOException e){
                            e.printStackTrace();
                        }
                }

            }
        }
       
    }

    public void updateRecentlyUsed() {
    	if (!recentlyUsed.isEmpty()){
    		for (PageId key : recentlyUsed.keySet()) {
    			int value = recentlyUsed.get(key);
    			value++;
    			recentlyUsed.put(key, value);	
    		}
    	}
    }

    /**
     * a LockManager class to keep track of locks. 
     */
    private class LockManager {

        private Map<PageId, Set<TransactionId>> pageReadLocks;
        private Map<PageId, TransactionId> pageWriteLocks;
        private Map<TransactionId, Set<PageId>> sharedPages;
        private Map<TransactionId, Set<PageId>> exclusivePages;


        /**
         *The constructor of the lockmanager
         */
        public LockManager(){
            pageReadLocks = new ConcurrentHashMap<PageId, Set<TransactionId>>();
            pageWriteLocks = new ConcurrentHashMap<PageId, TransactionId>();
            sharedPages = new ConcurrentHashMap<TransactionId, Set<PageId>>();
            exclusivePages = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        }

        /**
         * check to see if a transaction has a lock on a page
         * @param  tid specified TransactionID
         * @param  pid specified PageId
         * @return     true if tid has a lock on pid
         */
        public boolean holdsLock(TransactionId tid, PageId pid){
            Set<TransactionId> tidSet;
            TransactionId writetid; 
            tidSet = pageReadLocks.get(pid);
            writetid = pageWriteLocks.get(pid);
            return (tidSet.contains(tid) || writetid.equals(tid));
        }

        /**
         * release a transaction's lock on a page specified by pid
         * @param pid pageId of this page
         * @param tid TransactionId of this transaction
         */
        public synchronized void releaseLock(PageId pid, TransactionId tid){
            Set<PageId> pidSet = sharedPages.get(tid);
            Set<TransactionId> tidSet = pageReadLocks.get(pid);
            Set<PageId> exclusivePidSet = exclusivePages.get(tid);
            if (tidSet!= null){
                tidSet.remove(tid);
                pageReadLocks.put(pid, tidSet);
            }
            pageWriteLocks.remove(pid);
            if (pidSet != null){
                pidSet.remove(pid);
                sharedPages.put(tid, pidSet);
            }
            if (exclusivePidSet != null) {
                exclusivePidSet.remove(pid);
                exclusivePages.put(tid, exclusivePidSet);
            }
        }
		
		// ask LockManager to release all the locks related to a transaction, for transactionComplete()
        public synchronized void releaseAllTidLocks(TransactionId tid){
        	for (PageId pageId : pageWriteLocks.keySet()) {
        		if (pageWriteLocks.get(pageId) != null && pageWriteLocks.get(pageId)==tid) {
        			 pageWriteLocks.remove(pageId);
        		}
        	}
            exclusivePages.remove(tid);
    	
        	for (PageId pageId : pageReadLocks.keySet()) {
        		Set<TransactionId> tidSet = pageReadLocks.get(pageId);
        		if (tidSet != null) {
        			tidSet.remove(tid);
                    pageReadLocks.put(pageId, tidSet);

        		}
        	}
            sharedPages.remove(tid);

        	
	}

        public synchronized boolean grantLock(PageId pid, TransactionId tid, Permissions perm){

            if (perm.equals(Permissions.READ_ONLY)){
                Set<TransactionId> tidSet = pageReadLocks.get(pid);
                TransactionId writetid = pageWriteLocks.get(pid);
                if (writetid == null || writetid.equals(tid)) {

                    if (tidSet == null){
                        tidSet = new HashSet<TransactionId>();
                    }

                    tidSet.add(tid);
                    pageReadLocks.put(pid, tidSet);


                    Set<PageId> pageIdSet = sharedPages.get(tid);
                    if (pageIdSet == null) {
                        pageIdSet = new HashSet<PageId>();
                    }
                    pageIdSet.add(pid);
                    sharedPages.put(tid, pageIdSet);
                    return false;

                } else {
                    return true;
                }

                //If this is a Read and Write
            } else {
                Set<TransactionId> tidSet = pageReadLocks.get(pid);
                TransactionId writetid = pageWriteLocks.get(pid);

                if (tidSet != null && tidSet.size() > 1){
                    return true;
                }
                if (tidSet != null && tidSet.size() == 1 && !tidSet.contains(tid)){
                    return true;
                }
                if (writetid != null && !writetid.equals(tid)){
                    return true;
                } else {
                    pageWriteLocks.put(pid, tid);
                    Set<PageId> pidSet = exclusivePages.get(tid);
                    if (pidSet == null){
                        pidSet = new HashSet<PageId>();
                    }
                    pidSet.add(pid);
                    exclusivePages.put(tid, pidSet);
                    return false;

                }
            }
        }

    }
}

