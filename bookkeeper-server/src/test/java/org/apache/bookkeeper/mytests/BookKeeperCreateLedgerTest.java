package org.apache.bookkeeper.mytests;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Test;

public class BookKeeperCreateLedgerTest extends BookKeeperClusterTestCase {


    public BookKeeperCreateLedgerTest(){
        super(4);
    }

    @Test
    public void createLedgerTest() {

        try {
            LedgerHandle lh = bkc.createLedger(3, 1, BookKeeper.DigestType.CRC32, "ledgerPassword".getBytes());
            lh.addEntry(new byte[30], 0, 1);
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(2, 2);

    }

}