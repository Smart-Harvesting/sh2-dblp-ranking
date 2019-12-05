package de.th_koeln.iws.sh2.ranking;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Month;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import de.th_koeln.iws.sh2.ranking.analysis.ScoreCalculator;
import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceStream;
import de.th_koeln.iws.sh2.ranking.analysis.data.util.Calculator;
import de.th_koeln.iws.sh2.ranking.analysis.data.util.EvaluationConfiguration;
import de.th_koeln.iws.sh2.ranking.analysis.evaluation.RelevanceRanker;
import de.th_koeln.iws.sh2.ranking.core.DatabaseManager;
import de.th_koeln.iws.sh2.ranking.core.DbDataReader;

/**
 * 
 * Application that creates a ranking of dblp conferences for a specific
 * Year-Month.
 * 
 * @author neumannm
 *
 */
public class RankingApplication {

    private static Logger LOGGER = LogManager.getLogger(RankingApplication.class);

    private static final int EVAL_YEAR = 2018;
    private static final Month EVAL_MONTH = Month.AUGUST;

    private static void setRootLoggerLevel(Level level) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(level);
    }

    public static void main(String[] args) {

        setRootLoggerLevel(Level.DEBUG);

        Connection connection = connect("ranking.sqlite");

        createTableIfNotExists(connection);

        final Set<ConferenceStream> allConfs = getConferencesFromDatabase();
        LOGGER.info(String.format("Done reading %d conferences from database", allConfs.size()));

        EvaluationConfiguration config = createScoringConfiguration();
        final Map<ConferenceStream, Double> ranked = createRanking(allConfs, config, connection);

        for (Entry<ConferenceStream, Double> rankItem : ranked.entrySet()) {
            LOGGER.debug(rankItem.getKey() + " : " + rankItem.getValue());
        }
    }

    private static Connection connect(String sqlFileName) {
        String url = "jdbc:sqlite:" + sqlFileName; // TODO output folder from setup
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
            DatabaseMetaData meta = conn.getMetaData();
            LOGGER.debug("The driver name is " + meta.getDriverName());
            LOGGER.debug("A new database has been created.");
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }
        return conn;
    }

    private static void createTableIfNotExists(Connection conn) {
        // SQL statement for creating a new table
        String createTableStmt = "CREATE TABLE IF NOT EXISTS ranking (" + "conf_key text PRIMARY KEY NOT NULL,"
                + " score real NOT NULL," + " interval integer," + " month text," + " delay real," + " last_entry text,"
                + " expected text," + " activity real," + " rating real," + " prominence real,"
                + " internationality real," + " size real," + " affiliations real," + " log real" + ");";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableStmt);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }
    }

    private static EvaluationConfiguration createScoringConfiguration() {
        // activity and rating were best-performing factors in evaluation
        return new EvaluationConfiguration.Builder().useActivityScore().useRatingScore().build();
    }

    private static Map<ConferenceStream, Double> createRanking(Set<ConferenceStream> confs,
            EvaluationConfiguration config, Connection conn) {
        YearMonth evalYM = YearMonth.of(EVAL_YEAR, EVAL_MONTH);

        Map<ConferenceStream, Double> scored;

        ScoreCalculator scorer = new ScoreCalculator(config);
        scored = new TreeMap<>();

        /*
         * Go through _all_ conferences and score each one
         */
        for (ConferenceStream conf : confs) {
            Double score = scorer.getScore(conf, evalYM, conn);
            if (score != null)
                scored.put(conf, score);
        }
        return scored;
    }

    /**
     * Read the set of conferences to evaluate from the database.
     *
     * @return set of conferences
     */
    private static Set<ConferenceStream> getConferencesFromDatabase() {
        DbDataReader reader = new DbDataReader(DatabaseManager.getInstance());
        Set<ConferenceStream> data = reader.getData();
        return Collections.unmodifiableSet(data);
    }

}
