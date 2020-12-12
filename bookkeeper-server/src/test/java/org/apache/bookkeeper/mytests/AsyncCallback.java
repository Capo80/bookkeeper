package org.apache.bookkeeper.mytests;

import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;

import java.util.concurrent.Semaphore;

public class AsyncCallback implements org.apache.bookkeeper.client.AsyncCallback.DeleteCallback, BookkeeperInternalCallbacks.WriteCallback {

    //funcion called when delete operation is finished
    @Override
    public void deleteComplete(int rc, Object ctx) {
        ((DeleteControlObject)ctx).setRc(rc);
        ((DeleteControlObject)ctx).setDeleteComplete(true);
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
        ((WriteControlObject)ctx).setRc(rc);
        ((WriteControlObject)ctx).setLedgerId(ledgerId);
        ((WriteControlObject)ctx).setEntryId(entryId);
        ((WriteControlObject)ctx).setAddr(addr);
        ((WriteControlObject)ctx).setWriteComplete(true);
    }

    public static class WriteControlObject {

        private boolean writeComplete;
        private int rc;
        private long ledgerId;
        private long entryId;
        private BookieId addr;
        private Semaphore sem;

        public WriteControlObject() {
            this.writeComplete = false;
            this.sem = new Semaphore(0);
        }

        public boolean isWriteComplete(){
            return writeComplete;
        }

        public void setWriteComplete(boolean writeComplete) {
            this.writeComplete = writeComplete;
            sem.release();
        }

        public int getRc() {
            return rc;
        }

        public void setRc(int rc) {
            this.rc = rc;
        }

        public long getLedgerId() {
            return ledgerId;
        }

        public void setLedgerId(long ledgerId) {
            this.ledgerId = ledgerId;
        }

        public long getEntryId() {
            return entryId;
        }

        public void setEntryId(long entryId) {
            this.entryId = entryId;
        }

        public BookieId getAddr() {
            return addr;
        }

        public void setAddr(BookieId addr) {
            this.addr = addr;
        }

        public void waitWrite() throws InterruptedException {
            sem.acquire();
        }

    }


    public static class DeleteControlObject {

        private boolean deleteComplete;
        private int rc;
        private Semaphore sem;

        public DeleteControlObject() {
            this.deleteComplete = false;
            this.sem = new Semaphore(0);
        }

        public boolean isDeleteComplete(){
            return deleteComplete;
        }

        public void setDeleteComplete(boolean deleteComplete) {
            this.deleteComplete = deleteComplete;
            sem.release();
        }

        public int getRc() {
            return rc;
        }

        public void setRc(int rc) {
            this.rc = rc;
        }

        public void waitDelete() throws InterruptedException {
            sem.acquire();
        }


    }
}
