package de.th_koeln.iws.sh2.ranking.core;

import java.util.Collection;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Multimap;

import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceRecord;
import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceStream;

class DbDataReaderTest {

    static DbDataReader reader;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        reader = new DbDataReader(DatabaseManager.getInstance());
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {
    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    void testReadScores() {
        Collection<ConferenceStream> streams = DbDataReaderTest.reader.readScores();

        Assert.assertEquals(4658, streams.size());
    }

    @Test
    void testReadRecords() {
        Multimap<String, ConferenceRecord> readRecords = DbDataReaderTest.reader.readRecords();

        Assert.assertEquals(4395, readRecords.keySet().size());
    }

}
