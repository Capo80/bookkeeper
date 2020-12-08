package org.apache.bookkeeper.mytests;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

@RunWith(value = Parameterized.class)
public class BookKeeperDeleteLedgerTest extends BookKeeperClusterTestCase {

    private static LedgerHandle lh;
    private static CompletableFuture<Void> future;

    //arguments
    private boolean expResult;
    private long lId;
    private AsyncCallback.DeleteCallback cb;
    private final Object ctx;

    public BookKeeperDeleteLedgerTest(boolean expResult, long lId, AsyncCallback.DeleteCallback cb, Object ctx) {
        //Number of bookies is irrelevant in this test
        super(8);

        this.expResult = expResult;
        this.lId = lId;
        this.cb = cb;
        this.ctx = ctx;

    }

    @Before
    public void setUpLedger() {
        //Create the ledger we are trying to delete
        try {
            lh = bkc.createLedger(6, 5, 4, BookKeeper.DigestType.CRC32, "password".getBytes(),null);
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }

    }

    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        //function signature
        //void asyncDeleteLedger(final long lId, final DeleteCallback cb, final Object ctx)

        //the ID parameter is not strictly connected to the others, however we would like to see if both callback handle errors well so we test it with both configurations
        //The callback and control object are stricly related in my implementation, but the method allows them to be separated
        // i will use the bookKeeper implementation of the Callback to test the null control object

        //Create my callback and control object
        org.apache.bookkeeper.mytests.AsyncCallback cb1 = new org.apache.bookkeeper.mytests.AsyncCallback();
        org.apache.bookkeeper.mytests.AsyncCallback.DeleteControlObject ctx = new org.apache.bookkeeper.mytests.AsyncCallback.DeleteControlObject();

        //Create BookKeeper implementation of the callback
        future = new CompletableFuture<>();
        SyncCallbackUtils.SyncDeleteCallback cb2 = new SyncCallbackUtils.SyncDeleteCallback(future);

        return Arrays.asList(new Object[][]{

                //fail beacuse of negative or wrong id
                {false, -12345, cb1, ctx},
                {false, 12345, cb1, ctx},
                {false, -12345, cb2, null},
                {false, 12345, cb2, null},

                //fail because of no callback
                {false, 333, null, ctx},

                //valid configurations
                {true, 333, cb1, ctx},
                {true, 333, cb2, null},


        });

    }


    @Test
    public void deleteLedgerTest() {

        if (lId == 333)
            lId = lh.getId();

        bkc.asyncDeleteLedger(lId, cb, ctx);

        if (ctx == null) {
            //Using BookKeeper callback
            try {
                SyncCallbackUtils.waitForResult(future);
            } catch (InterruptedException e) {
                //we failed beacause of a system problem
                e.printStackTrace();
                Assert.fail();
            } catch (BKException e) {
                //we failed to delete the ledger - check that the error is correct
                Assert.assertEquals(e.getMessage() ,"No such ledger exists on Metadata Server");
            }

        } else {
            //Using my callback
            synchronized (((org.apache.bookkeeper.mytests.AsyncCallback.DeleteControlObject) ctx)) {
                while (!((org.apache.bookkeeper.mytests.AsyncCallback.DeleteControlObject) ctx).isDeleteComplete()) {
                    try {
                        ((org.apache.bookkeeper.mytests.AsyncCallback.DeleteControlObject) ctx).waitDelete();
                    } catch (InterruptedException e) {
                        // system failure
                        e.printStackTrace();
                        Assert.fail();
                    }
                }
            }

            //the operation should always succed - even when the ledger does not exist
            Assert.assertEquals(((org.apache.bookkeeper.mytests.AsyncCallback.DeleteControlObject) ctx).getRc(), BKException.Code.OK);

        }

        //if we are here the ledger should have been deleted
        //lets try to open it to see if its actually gone
        try {
            LedgerHandle lh = bkc.openLedger(lId, BookKeeper.DigestType.CRC32, "password".getBytes());
            //lh.addEntry("ciao".getBytes());
        } catch (InterruptedException e) {
            //we failed beacause of a system problem
            e.printStackTrace();
            Assert.fail();
        } catch (BKException e) {
            //if we successfully deleted the ledger we should get this error
            Assert.assertEquals(e.getMessage() ,"No such ledger exists on Metadata Server");
            return;
        }

        //if we reach here the ledger has been opened successfully which means we failed to delete it
        Assert.fail();
    }

    @After
    public void deleteLedger() {
        //If we fail to delete the ledger we do it here
        try {
            if (!expResult)
                bkc.deleteLedger(lh.getId());
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }
    }

}
