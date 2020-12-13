package org.apache.bookkeeper.mytests;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.bookie.Bookie;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

@RunWith(value = Parameterized.class)
public class BookieReadEntryTest extends BookKeeperClusterTestCase {

    private static LedgerHandle lh;
    private static ByteBuf entryData;
    private final static long EXISTING_ENTRY_ID = 0;

    //bookie to test
    private Bookie bookie;

    //arguments
    private boolean expResult;
    private long lId;
    private long entryId;

    public BookieReadEntryTest(boolean expResult, long lId, long entryId) {
        super(8);

        this.expResult = expResult;
        this.lId = lId;
        this.entryId = entryId;

    }

    @Before
    public void setUpLedger() throws Exception {

        //get one bookie to test
        this.bookie = bs.get(0).getBookie();

        //Create the ledger where we are going to read the entry and add an entry
        try {
            lh = bkc.createLedger(8, 8, 8, BookKeeper.DigestType.CRC32, "password".getBytes(),null);
            entryData = Utils.buildEntry(lh.getId(), EXISTING_ENTRY_ID, "data".getBytes());
            bookie.addEntry(entryData,true, null, null, "key".getBytes());
        } catch (InterruptedException | BKException e) {
            //system error
            e.printStackTrace();
            Assert.fail();
        }


    }
    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        //function signature
        //void ByteBuf readEntry(long ledgerId, long entryId)
        //            throws IOException, NoLedgerException

        //Most arguments appear to be unrelated, we can test them unidimensionally

        return Arrays.asList(new Object[][]{

                //bad configurations - wrong ID or wrong entry
                {false, 1234, EXISTING_ENTRY_ID},
                {false, 333, 1234},

                //valid configuration
                {false, 333, EXISTING_ENTRY_ID}

        });

    }
    @Test
    public void bookieReadEntryTest() {

        entryData.retain();
        if (lId == 333)
            lId = lh.getId();

        ByteBuf readEntry;
        try {
            readEntry = bookie.readEntry(lId, entryId);
            readEntry.retain();
        } catch (IOException e) {
            if (lId == 1234)
                Assert.assertEquals(e.getMessage(), "Ledger 1234 not found");
            else if (entryId == 1234)
                Assert.assertEquals(e.getMessage(), "Entry 1234 not found in 0");
            else
                Assert.fail();
            return;
        }

        //if we are here we have successfully read the entry, check if its correct
        Assert.assertEquals(entryData, readEntry);
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
