package org.apache.bookkeeper.mytests;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@RunWith(value = Parameterized.class)
public class BookKeeperOpenLedgerTest extends BookKeeperClusterTestCase {

    private static LedgerHandle lh;

    //arguments
    private boolean expResult;
    private long lId;
    private BookKeeper.DigestType digestType;
    private byte[] password;

    public BookKeeperOpenLedgerTest(boolean expResult, long lId, BookKeeper.DigestType digestType, byte[] password) {
        //Number of bookies is irrelevant in this test
        super(8);

        this.expResult = expResult;
        this.lId = lId;
        this.digestType = digestType;
        this.password = password;
    }

    @Before
    public void setUpLedger() {
        //Create the ledger we are trying to open
        try {
            lh = bkc.createLedger(6, 5, 4, BookKeeper.DigestType.CRC32, "password".getBytes(),null);
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }
    }

    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        //function signature
        //LedgerHandle openLedger(long lId, DigestType digestType, byte[] passwd)

        //this parameters can be tested unidimensionally, they seem to have no connection between each other

        return Arrays.asList(new Object[][]{

                //fail beacuse of negative or wrong id
                {false, -12345, BookKeeper.DigestType.MAC, "password".getBytes()},
                {false, 12345, BookKeeper.DigestType.MAC, "password".getBytes()},

                //fail because of wrong password
                {false, 333, BookKeeper.DigestType.MAC, "bad_password".getBytes()},
                {false, 333, BookKeeper.DigestType.MAC, "".getBytes()},

                //valid configurations
                {true, 333, BookKeeper.DigestType.MAC, "password".getBytes()},
                {true, 333, BookKeeper.DigestType.CRC32C, "password".getBytes()},
                {true, 333, BookKeeper.DigestType.CRC32, "password".getBytes()},
                {true, 333, BookKeeper.DigestType.DUMMY, "password".getBytes()},



        });

    }

    @Test
    public void openLedgerTest() {

        if (lId == 333)
            lId = lh.getId();
        try {

            LedgerHandle lha = bkc.openLedger(lId, digestType, password);

            //check if ledger is open for us (which means its closed for others)
            Assert.assertTrue(lha != null && lha.isClosed());

        } catch (InterruptedException e) {
            //the test failed becuse of a system failure
            e.printStackTrace();
            Assert.fail();
        } catch (BKException e) {
            //we failed to open the ledger - check if the error is correct
            if (!Arrays.equals(password, "password".getBytes()))
                Assert.assertEquals(e.getMessage() ,"Attempted to access ledger using the wrong password");
            else
                Assert.assertEquals(e.getMessage() ,"No such ledger exists on Metadata Server");
        }

    }

    @After
    public void deleteLedger() {
        //Delete the ledger that we have created
        try {
            bkc.deleteLedger(lh.getId());
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }
    }

}
