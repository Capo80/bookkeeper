package org.apache.bookkeeper.mytests;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.commons.collections4.set.ListOrderedSet;
import org.bouncycastle.crypto.Digest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.*;
import org.slf4j.Logger;

@RunWith(value = Parameterized.class)
public class BookKeeperCreateLedgerTest extends BookKeeperClusterTestCase {

    private static final Logger logger = LoggerFactory.getLogger(BookKeeperCreateLedgerTest.class);

    //parameters
    private boolean expResult;
    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;
    private Map<String, byte[]> customMetadata;

    public BookKeeperCreateLedgerTest(boolean expResult, int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd, final Map<String, byte[]> customMetadata){

        //super constructor will create a bookkeeper istance with 8 bookies
        //actual number of bookies is irrelevant
        //will modify parameters accordigly
        super(8);

        logger.error("parameters: " + ensSize + " " + writeQuorumSize + " " + ackQuorumSize + " " + digestType + " " + passwd + " " + customMetadata);

        this.expResult = expResult;
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.digestType = digestType;
        this.passwd = passwd;
        this.customMetadata = customMetadata;
    }



    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {

        // function signature
        // LedgerHandle createLedger(int ensSize, int writeQuorumSize, int ackQuorumSize,
        //                                     DigestType digestType, byte[] passwd, final Map<String, byte[]> customMetadata

        // The arguments are mostly unrelated and with a lot of partition, a multidimensional approach would too expensive and probrably not necessary
        // we proceed unidimensionally with particular attention to the parameters of ackQuorum and writeQuorum

        //Prepare metadata argument
        Map<String, byte[]> validMetadata = new HashMap<>();

        validMetadata.put("dummyKey1", "dummyValue1".getBytes());
        validMetadata.put("dummyKey2", "dummyValue2".getBytes());

        return Arrays.asList(new Object[][]{

                //fail beacuse of negative/0 ensSize
                {false, -1 , -1, -1, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},
                {false, 4 , -1, -1, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},
                {false, 0 , 0, 0, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},
                {false, 4 , 0, 0, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},

                //valid configrations
                {true, 4 , 2, 1, BookKeeper.DigestType.MAC, "".getBytes(), validMetadata},
                {true, 4 , 2, 1, BookKeeper.DigestType.CRC32, "password".getBytes(), validMetadata},
                {true, 4 , 2, 1, BookKeeper.DigestType.CRC32C, "password".getBytes(), null},
                {true, 4 , 2, 1, BookKeeper.DigestType.DUMMY, "password".getBytes(), Collections.emptyMap()},
                {true, 8 , 2, 1, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},
                {true, 8 , 8, 1, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},
                {true, 8 , 8, 8, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},
                {true, 4 , 2, -1, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},
                {true, 4 , 2, 0, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},

                //fail becuase ensSize, write or ack quorum are bigger than the actual number of live bookies
                {false, 10 , 2, 1, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},
                {false, 10 , 10, 2, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},
                {false, 10 , 10, 10, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},

                //fail becuse ensSize < writeQuorum
                {false, 4 , 5, 3, BookKeeper.DigestType.MAC, "password".getBytes(), validMetadata},

        });


    }

    @Test
    public void createLedgerTest() {

        try {
            System.out.println(ensSize + " " + writeQuorumSize + " " + ackQuorumSize + " " + digestType + " " + passwd + " " + customMetadata);
            LedgerHandle lh = bkc.createLedger(ensSize, writeQuorumSize,ackQuorumSize,digestType,passwd,customMetadata);


            if (lh != null && expResult)
                //ledger created as expected
                return;
            else if (lh == null && !expResult)
                //ledger not created as expected
                return;
            else
                //unexpected result, test fails

                if (lh != null) {
                    //lh.addEntry("pippo".getBytes());
                    logger.error("parameters before fail: " + lh.getLedgerKey() + " " + expResult + " " + ensSize + " " + writeQuorumSize + " " + ackQuorumSize + " " + digestType + " " + passwd + " " + customMetadata);
                }
                Assert.fail();

        } catch (IllegalArgumentException e) {
            if (expResult)
                //expected a ledger creation, instead got an exeption
                Assert.fail();
        } catch (InterruptedException | BKException e) {
            //The test failed beacause of a system failure
            Assert.fail();
        }
    }

}