package org.apache.bookkeeper.mytests;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.SyncCallbackUtils;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.matchers.Null;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

@RunWith(value = Parameterized.class)
public class BookieAddEntryTest extends BookKeeperClusterTestCase {

    private static LedgerHandle lh;
    private static CompletableFuture<Void> future;

    //bookie to test
    private Bookie bookie;

    //arguments
    private boolean expResult;
    private ByteBuf entry;
    private boolean ackBeforeSync;
    private BookkeeperInternalCallbacks.WriteCallback cb;
    private Object ctx;
    private byte[] masterKey;

    public BookieAddEntryTest(boolean expResult, ByteBuf entry, boolean ackBeforeSync, BookkeeperInternalCallbacks.WriteCallback cb, Object ctx, byte[] masterKey) {
        super(8);

        this.expResult = expResult;
        this.entry = entry;
        this.ackBeforeSync = ackBeforeSync;
        this.cb = cb;
        this.ctx = ctx;
        this.masterKey = masterKey;

    }

    @Before
    public void setUpLedger() throws Exception {

        //get one bookie to test
        this.bookie = bs.get(0).getBookie();

        //Create the ledger where we are going to add the entry
        try {
            lh = bkc.createLedger(8, 8, 8, BookKeeper.DigestType.CRC32, "password".getBytes(),null);
            System.out.println("afetr creation:" + lh.getId());
        } catch (InterruptedException | BKException e) {
            //system error
            e.printStackTrace();
            Assert.fail();
        }

    }


    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        //function signature
        //void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey)
        //            throws IOException, BookieException, InterruptedException

        //Most arguments appear to be unrelated, we can test them unidimensionally
        //The callback and control object are strictly related in my implementation, but the method allows them to be separated
        // i will use the bookKeeper implementation of the Callback to test the null control object

        //Create my callback and control object
        AsyncCallback cb1 = new org.apache.bookkeeper.mytests.AsyncCallback();
        AsyncCallback.WriteControlObject ctx = new AsyncCallback.WriteControlObject();

        //Create BookKeeper implementation of the callback
        future = new CompletableFuture<>();
        SyncCallbackUtils.SyncWriteCallback cb2 = new SyncCallbackUtils.SyncWriteCallback(future);

        //Create data
        ByteBuf entry1 = Utils.buildEntry(1234, 0);
        ByteBuf entry2 = Utils.buildEntry(-1234, 1);
        ByteBuf entry3 = Utils.buildEntry(333, 0);
        ByteBuf entryWithData = Utils.buildEntry(333, 0, "data".getBytes());
        ByteBuf badEntry = Utils.buildEntry(333, -1);

        return Arrays.asList(new Object[][]{

                //bad configurations - entry cannot be null
                {false, null, true, cb2, null, "key".getBytes()},
                {false, badEntry, true, cb2, null, "key".getBytes()},

                //valid configurations
                {true, entry3, true, cb2, null, "key".getBytes()},
                {true, entry3, false, cb2, null, "key".getBytes()},
                {true, entry3, true, cb1, ctx, "key".getBytes()},
                {true, entry3, true, cb2, null, "".getBytes()},
                {true, entryWithData, true, cb2, null, "".getBytes()},

                //valid configuration, a new ledger is created when it does not exists
                {true, entry1, true, cb2, null, "key".getBytes()},
                {true, entry2, true, cb2, null, "key".getBytes()},


        });

    }

    @Test
    public void bokkieAddEntryTest() {

        if (entry != null)
            entry.retain();
        //Set ledger id correctly
        if (entry != null && (entry.getLong(entry.readerIndex()) == 333)) {
            entry.setLong(entry.readerIndex(), lh.getId());
        }

        try {
            bookie.addEntry(entry,ackBeforeSync,cb,ctx,masterKey);
            System.out.println("Added entry" + entry.getLong(entry.readerIndex()));
        } catch (IOException | InterruptedException e) {
            //System error
            e.printStackTrace();
            Assert.fail();
        } catch (BookieException e) {
            e.printStackTrace();
            Assert.fail();
        } catch (NullPointerException e) {
            if (entry == null)
                return;
        }

        if (cb != null)
            if (ctx == null) {
                //Using BookKeeper callback
                try {
                    SyncCallbackUtils.waitForResult(future);
                } catch (InterruptedException e) {
                    //we failed beacause of a system problem
                    e.printStackTrace();
                    Assert.fail();
                } catch (BKException e) {
                    //we failed to add the entry
                    e.printStackTrace();
                    Assert.fail();
                }

            } else {
                //Using my callback
                synchronized (((AsyncCallback.WriteControlObject) ctx)) {
                    while (!((AsyncCallback.WriteControlObject) ctx).isWriteComplete()) {
                        try {
                            ((AsyncCallback.WriteControlObject) ctx).waitWrite();
                        } catch (InterruptedException e) {
                            // system failure
                            e.printStackTrace();
                            Assert.fail();
                        }
                    }
                }

                //Check the result of the operation
                if (expResult)
                    Assert.assertEquals(((AsyncCallback.WriteControlObject) ctx).getRc(), BKException.Code.OK);
                else    
                    Assert.assertNotEquals(((AsyncCallback.WriteControlObject) ctx).getRc(), BKException.Code.OK);

            }
        else {
            //if we dont pass a callback the operation still succedes, we just cant get notified of it
            //i sleep for a bit to give time to the operation
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Assert.fail();
            }
        }

        //check if the entry has been correctly added
        try {

            ByteBuf readEntry = bookie.readEntry(entry.getLong(entry.readerIndex()), entry.getLong(entry.readerIndex()+8));

            if (expResult) {
                System.out.println("Before reading" + entry + readEntry);
                Assert.assertEquals(readEntry, entry);
            } else
                Assert.fail();

        } catch (IOException e) {
            e.printStackTrace();
            // entryId is -1 -we cannot find it
            Assert.assertEquals(e.getMessage() ,"Entry 0 not found in 0");
        }

    }

    @After
    public void deleteLedger() {
        //delete the ledger after
        try {
            bkc.deleteLedger(lh.getId());
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }
    }
}
