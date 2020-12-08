package org.apache.bookkeeper.mytests;

import java.util.concurrent.Semaphore;

public class AsyncCallback implements org.apache.bookkeeper.client.AsyncCallback.DeleteCallback {

    //funcion called when delete operation is finished
    @Override
    public void deleteComplete(int rc, Object ctx) {
        ((DeleteControlObject)ctx).setDeleteComplete(true);
        ((DeleteControlObject)ctx).setRc(rc);
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
