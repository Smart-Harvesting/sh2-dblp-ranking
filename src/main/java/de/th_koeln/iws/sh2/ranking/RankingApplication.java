package de.th_koeln.iws.sh2.ranking;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Month;
import java.time.YearMonth;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
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
import de.th_koeln.iws.sh2.ranking.analysis.data.util.EvaluationConfiguration;
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

        setRootLoggerLevel(Level.INFO);

        final Set<ConferenceStream> allConfs = getConferencesFromDatabase();
        LOGGER.info(String.format("Done reading %d conferences from database", allConfs.size()));

        EvaluationConfiguration config = createScoringConfiguration();
        StringBuffer buffer = new StringBuffer();
        final Map<ConferenceStream, Double> ranked = createRanking(allConfs, config, buffer);

        for (Entry<ConferenceStream, Double> rankItem : ranked.entrySet()) {
            LOGGER.debug(rankItem.getKey() + " : " + rankItem.getValue());
        }
        writeResultFile(buffer, config.toString());
    }

    private static void writeResultFile(StringBuffer buffer, String filename) {
        try (BufferedWriter bwr = new BufferedWriter(new FileWriter(new File(filename + ".json")))) {
            bwr.write(buffer.toString());
            bwr.flush();
            bwr.close();
        } catch (IOException e) {
            LOGGER.error("Error writing JSON to file.", e);
        }
    }

    private static EvaluationConfiguration createScoringConfiguration() {
        // activity and rating were best-performing factors in evaluation
        return new EvaluationConfiguration.Builder()
                // .useActivityScore()
                // .useRatingScore()
                .build();
    }

    private static Map<ConferenceStream, Double> createRanking(Set<ConferenceStream> confs,
            EvaluationConfiguration config, StringBuffer jsonResult) {
        YearMonth evalYM = YearMonth.of(EVAL_YEAR, EVAL_MONTH);

        Map<ConferenceStream, Double> scored;

        ScoreCalculator scorer = new ScoreCalculator(config);
        scored = new TreeMap<>();

        /*
         * Go through _all_ conferences and score each one
         */

        jsonResult.append("[\n");
        Iterator<ConferenceStream> iterator = confs.iterator();
        while(iterator.hasNext()) {
            ConferenceStream conf  = iterator.next();
            Double score = scorer.getScore(conf, evalYM, jsonResult);
            if (score != null)
                scored.put(conf, score);
            if ((score > 0.0) && iterator.hasNext())
                jsonResult.append(",\n");
        }
        jsonResult.append("]");
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
