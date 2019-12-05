package de.th_koeln.iws.sh2.ranking.analysis;

import static java.time.temporal.ChronoUnit.MONTHS;

import java.time.Month;
import java.time.Year;
import java.time.YearMonth;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.TreeMultimap;

import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceStream;
import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceRecord;
import de.th_koeln.iws.sh2.ranking.analysis.data.util.Calculator;
import de.th_koeln.iws.sh2.ranking.analysis.data.util.EvaluationConfiguration;

/**
 * Calculator for scoring {@link ConferenceStream}s.
 *
 * @author neumannm
 *
 */
public class ScoreCalculator {

    private static Logger LOGGER = LogManager.getLogger(ScoreCalculator.class);

    private static final int DELTA_MEDIAN_SAMPLE_SIZE = 5;

    private EvaluationConfiguration config;

    /**
     * Constructor.
     *
     * @param config set to true if only raw baseline score should be calculated.
     */
    public ScoreCalculator(EvaluationConfiguration config) {
        this.config = config;
    }

    /**
     * Calculate a score for a conference for a given point in time (MM-YYYY).
     *
     * @param conf      the conference to be scored
     * @param testyear  the evaluation year
     * @param testmonth the evaluation month
     * @return conference score, will be 0.0 if there are no records older than the
     *         test year or if next conference entry is not yet expected
     */
    public Double getScore(ConferenceStream conf, YearMonth evalYM) {
        double minValue = 0.0;

        // get all records of this conference that have been created before the given
        // year
        TreeMultimap<YearMonth, ConferenceRecord> recordsCreatedBefore = conf
                .getRecordsCreatedBefore(Year.of(evalYM.getYear()));

        String streamKey = conf.getKey();
        LOGGER.debug("[{}, {}] eventYM list: {}", evalYM, streamKey, recordsCreatedBefore.keySet());

        if (recordsCreatedBefore.isEmpty())
            // no records available that are from before the test year - conf not relevant
            return minValue;

        // calculate usual delay (median of delays)
        long medianDelay = Calculator.calcMedianDelay(recordsCreatedBefore, DELTA_MEDIAN_SAMPLE_SIZE);
        LOGGER.debug("[{}, {}] median delay: {}", evalYM, streamKey, medianDelay);
        // calculate usualMonth (mode of months)
        Month modeMonth = Calculator.calcModeMonth(recordsCreatedBefore, DELTA_MEDIAN_SAMPLE_SIZE);
        LOGGER.debug("[{}, {}] mode month: {}", evalYM, streamKey, modeMonth);
        // calculate usual interval (median of intervals)
        long medianInterval = Calculator.calcMedianInterval(recordsCreatedBefore, DELTA_MEDIAN_SAMPLE_SIZE);
        LOGGER.debug("[{}, {}] interval: {}", evalYM, streamKey, medianInterval);

        // the latest record year
        YearMonth lastYearMonth = recordsCreatedBefore.keySet().first();
        LOGGER.debug("[{}, {}] latest record year: {}", evalYM, streamKey, lastYearMonth);

        /*
         * next entry is expected in 'interval' years from last entry and in
         * 'medianDelay' from the 'avgMonth'
         *
         * e.g. if last entry was from 2015 and normally conference is in May, the
         * average delay is 2 months, and the interval is 1, then next entry is expected
         * in July 2016
         *
         * also works across year boundaries: e.g. if month is normally November and the
         * delay is usually 3 months, entry of 2016 is expected in February 2017
         */
        YearMonth expectedNextEntry = YearMonth.of(lastYearMonth.getYear(), modeMonth.getValue())
                .plusMonths(medianInterval).plusMonths(medianDelay);
        LOGGER.debug("[{}, {}] expected next entry: {}", evalYM, streamKey, expectedNextEntry);

        if (!evalYM.isBefore(expectedNextEntry)) { // check if conference should be expected
            // the base score is the interval factor based on the current delay
            long delay = expectedNextEntry.until(evalYM, MONTHS);
            LOGGER.debug("[{}, {}] raw delay in months: {}", evalYM, streamKey, delay);

            final double delayFactor = this.getDelayFactor(delay);
            LOGGER.debug("[{}, {}] delay factor score: {}", evalYM, streamKey, delayFactor);

            double activityFactor = 1.0, ratingFactor = 1.0, intlFactor = 1.0, citeFactor = 1.0, prominenceFactor = 1.0,
                    sizeFactor = 1.0, affilFactor = 1.0, logFactor = 1.0;

            if (this.config.isUseActivityScore()) {
                // intervalAge = eventAgeInMonths / medianIntervalInMonths;
                final double intervalAge = lastYearMonth.until(evalYM, MONTHS) / (double) medianInterval;
                LOGGER.debug("[{}, {}] interval age in months: {}", evalYM, streamKey, intervalAge);
                // active(c) = 1 / (1 + intervalAge^2) global max 1 with intervalAge = 0;
                // lim(activity(c)) = 0 for intervalAge -> +Infinity;
                final double activityScore = 1d / (1d + (intervalAge * intervalAge));
                // w_active(c) = 1 + active(c);
                activityFactor = 1 + activityScore;
                LOGGER.debug("[{}, {}] activity factor score: {}", evalYM, streamKey, activityFactor);
            }

            if (this.config.isUseSizeScore()) {
                Double avgSize = conf.getAvgSize();
                if (null == avgSize) {
                    LOGGER.warn("No size score available for conf '" + streamKey + "'");
                } else {
                    sizeFactor = 1 + avgSize;
                }

                LOGGER.debug("[{}, {}] size factor score: {}", evalYM, streamKey, sizeFactor);
            }

            if (this.config.isUseRatingScore()) {
                Double avgRating = conf.getAvgRating();
                if (null != avgRating) { // a lot of conferences have no rating info available
                    ratingFactor = 1 + avgRating;
                }

                LOGGER.debug("[{}, {}] rating factor score: {}", evalYM, streamKey, ratingFactor);
            }

            if (this.config.isUseIntlScore()) {
                Double intlScore = conf.getIntlScore();
                if (null == intlScore) {
                    LOGGER.warn("No internationality score available for conf '" + streamKey + "'");
                } else {
                    intlFactor = 1 + intlScore;
                }

                LOGGER.debug("[{}, {}] internationality factor score: {}", evalYM, streamKey, intlFactor);
            }

            if (this.config.isUseCitationScore()) {
                Double citationsScore = conf.getCitationsScore();
                if (null == citationsScore) {
                    LOGGER.warn("No citations score available for conf '" + streamKey + "'");
                } else {
                    citeFactor = 1 + citationsScore;
                }

                LOGGER.debug("[{}, {}] cite factor score: {}", evalYM, streamKey, citeFactor);
            }

            if (this.config.isUsePromScore()) {
                Double prominence = conf.getProminence();
                if (null == prominence) {
                    LOGGER.warn("No prominence score available for conf '" + streamKey + "'");
                } else {
                    prominenceFactor = 1 + prominence;
                }

                LOGGER.debug("[{}, {}] prominence factor score: {}", evalYM, streamKey, prominenceFactor);
            }

            if (this.config.isUseAffilScore()) {
                Double affilScore = conf.getAffilScore();
                if (null == affilScore) {
                    // LOGGER.warn("No affiliation score available for conf '" + conf.getKey() +
                    // "'");
                } else {
                    affilFactor = 1 + affilScore;
                }

                LOGGER.debug("[{}, {}] affiliation factor score: {}", evalYM, streamKey, affilFactor);
            }

            if (this.config.isUseLogScore()) {
                // get log score of the month before the test month
                Double logScore = conf.getLogScores().get(evalYM.minusMonths(1));
                if (null != logScore) {
                    LOGGER.debug("Log score available for conf '" + streamKey + "'");
                    logFactor = 1 + logScore;
                }

                LOGGER.debug("[{}, {}] log factor score: {}", evalYM, streamKey, logFactor);
            }

            /*
             * score formula:
             *
             * score = (delay/dead²) * (1+avgRank) * (1+internationality) * (1+citations)
             * *(1+prominence)
             */
            double score = delayFactor * activityFactor * sizeFactor * ratingFactor * intlFactor * citeFactor
                    * prominenceFactor * affilFactor * logFactor;
            LOGGER.debug("[{}, {}] final score: {}", evalYM, streamKey, score);
            return score;
        }
        return minValue;
    }

    private double getDelayFactor(long delay) {
        if (delay >= 0) {
            return 1.0 + (1.0 / (1.0 + (Math.log1p(delay) / Math.log(2))));
        }
        return -1;
    }
}
