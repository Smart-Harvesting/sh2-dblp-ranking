package de.th_koeln.iws.sh2.ranking.core;

import java.sql.SQLException;
import java.util.Collection;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceStream;

class DbDataReaderTest {

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
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
    void test() {
        DbDataReader reader = new DbDataReader(DatabaseManager.getInstance());

        try {
            Collection<ConferenceStream> streams = reader.readScores();

            Assert.assertTrue(streams.size() == 4658);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
