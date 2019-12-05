package de.th_koeln.iws.sh2.ranking.core;

import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.AFFILIATON_SCORE;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.CITATION_SCORE;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.CITE_KEY;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.C_DATE;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.EVENT_COUNTRY;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.EVENT_MONTH;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.EVENT_YEAR;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.INSERTION_DELAY;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.INTL_SCORE;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.PROMINENCE_SCORE;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.PUBLICATION_YEAR;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.RATING_SCORE;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.RECORD_KEY;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.RECORD_TITLE;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.RECORD_TYPE;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.SIZE_SCORE;
import static de.th_koeln.iws.sh2.ranking.core.util.ColumnNames.STREAM_KEY;
import static de.th_koeln.iws.sh2.ranking.core.util.TableNames.HISTORICAL_RECORDS;
import static de.th_koeln.iws.sh2.ranking.core.util.TableNames.RAW_LOG_SCORES;
import static de.th_koeln.iws.sh2.ranking.core.util.TableNames.SCORES;
import static de.th_koeln.iws.sh2.ranking.core.util.ViewNames.PROCEEDINGS_VIEW;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Table;

import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceRecord;
import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceStream;
import de.th_koeln.iws.sh2.ranking.analysis.data.Type;

public class DbDataReader implements DataReader {

    private static Logger LOGGER = LogManager.getLogger(DbDataReader.class);

    private DatabaseManager dbm;

    private Set<ConferenceStream> conferenceStreamSet;

    final String logFormat = "{}: Reading data from table '{}'";

    public DbDataReader(DatabaseManager dbm) {
        this.dbm = dbm;
    }

    @Override
    public Set<ConferenceStream> getData() {
        if (null == this.conferenceStreamSet) {
            this.readFromDatabase();
        }
        return this.conferenceStreamSet;
    }

    private void readFromDatabase() {
        Multimap<String, ConferenceRecord> streamRecords = this.readRecords();
        Set<ConferenceStream> conferencescores = this.readScores();
        Table<String, YearMonth, Double> streamLogScores = this.readStreamLogScores();

        for (ConferenceStream conferenceStream : conferencescores) {
            String key = conferenceStream.getKey();
            conferenceStream.setRecords(streamRecords.get(key));
            conferenceStream.setLogScores(streamLogScores.row(key));
        }

        this.conferenceStreamSet = Collections.unmodifiableSet(conferencescores);
    }

    protected Set<ConferenceStream> readScores() {
        LOGGER.info(this.logFormat, "START", SCORES);
        final Instant start = Instant.now();

        final Connection connection = this.dbm.getConnection();

        Statement stmt = null;
        ResultSet rs = null;
        String selectMaxString = "SELECT MAX(%s) " + "FROM " + SCORES;
        String selectString = "SELECT * " + "FROM " + SCORES;

        Collection<ConferenceStream> streams = new ArrayList<>();
        try {
            connection.setAutoCommit(false);

            stmt = connection.createStatement();

            double maxIntlScore = this.queryMaximum(String.format(selectMaxString, INTL_SCORE));
            LOGGER.debug("Internationality maximum: {}", maxIntlScore);

            double maxAffilScore = this.queryMaximum(String.format(selectMaxString, AFFILIATON_SCORE));
            LOGGER.debug("Affiliation score maximum: {}", maxIntlScore);

            double maxRatingScore = this.queryMaximum(String.format(selectMaxString, RATING_SCORE));
            LOGGER.debug("Rating score maximum: {}", maxIntlScore);

            double maxCiteScore = this.queryMaximum(String.format(selectMaxString, CITATION_SCORE));

            double maxPromScore = this.queryMaximum(String.format(selectMaxString, PROMINENCE_SCORE));

            double maxSizeScore = this.queryMaximum(String.format(selectMaxString, SIZE_SCORE));

            /*
             * get all results
             */
            stmt.executeQuery(selectString);
            connection.commit();

            rs = stmt.getResultSet();

            while (rs.next()) {
                String streamKey = rs.getString(STREAM_KEY);
                // SQL NULL values are returned as 0, so division is safe
                Double intScore = rs.getDouble(INTL_SCORE) / maxIntlScore;
                Double affilScore = rs.getDouble(AFFILIATON_SCORE) / maxAffilScore;
                Double ratingScore = rs.getDouble(RATING_SCORE) / maxRatingScore;
                Double citationScore = rs.getDouble(CITATION_SCORE) / maxCiteScore;
                Double prominenceScore = rs.getDouble(PROMINENCE_SCORE) / maxPromScore;
                Double sizeScore = rs.getDouble(SIZE_SCORE) / maxSizeScore;

                ConferenceStream conf = new ConferenceStream(streamKey);
                conf.setAffilScore(affilScore);
                conf.setCitScore(citationScore);
                conf.setIntlScore(intScore);
                conf.setPromScore(prominenceScore);
                conf.setRatingScore(ratingScore);
                conf.setSizeScore(sizeScore);

                streams.add(conf);
            }

            return new HashSet<>(streams);
        } catch (SQLException e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    LOGGER.error("Transaction is being rolled back");
                    connection.rollback();
                } catch (SQLException excep) {
                    e.printStackTrace();
                }
            }
        } finally {
            try {
                LOGGER.info(this.logFormat + " (Duration: {})", "END", SCORES, Duration.between(start, Instant.now()));
                if (stmt != null) {
                    stmt.close();
                }
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return new HashSet<>(streams);
    }

    private Double queryMaximum(String selectMaxString) throws SQLException {
        final Instant start = Instant.now();

        final Connection connection = this.dbm.getConnection();

        Statement stmt = null;
        ResultSet rs = null;
        try {
            connection.setAutoCommit(false);

            stmt = connection.createStatement();

            /*
             * get max value
             */
            stmt.executeQuery(selectMaxString);
            connection.commit();
            rs = stmt.getResultSet();

            double maxValue = -1.0;
            // get the single value returned by that statement
            if (rs.next()) {
                maxValue = rs.getDouble(1);
                return maxValue;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    LOGGER.error("Transaction is being rolled back");
                    connection.rollback();
                } catch (SQLException excep) {
                    e.printStackTrace();
                }
            }
        } finally {
            LOGGER.info("{} Reading maximum" + " (Duration: {})", "END", Duration.between(start, Instant.now()));
            if (stmt != null)
                stmt.close();
        }
        return null;
    }

    protected Multimap<String, ConferenceRecord> readRecords() {

        LOGGER.info(this.logFormat, "START", "joined table");
        final Instant start = Instant.now();

        final Multimap<String, ConferenceRecord> toReturn = MultimapBuilder.hashKeys().arrayListValues().build();

        Statement stmt = null;
        ResultSet rs = null;

        /*
         * we only get streams and their records where the event month and year of the
         * record is known, as we need this information to estimate the delay between
         * conference and entry to dblp later on
         */
        String innerSelect = String.format(
                "SELECT procs.%s, procs.%s, hist.%s, hist.%s, hist.%s, hist.%s, hist.%s, hist.%s, hist.%s, hist.%s "
                        + "FROM %s procs " + "JOIN %s hist " + "ON procs.%s = hist.%s",

                        STREAM_KEY, CITE_KEY, C_DATE, RECORD_TITLE, RECORD_TYPE, PUBLICATION_YEAR, EVENT_MONTH, EVENT_YEAR,
                        EVENT_COUNTRY, INSERTION_DELAY,

                        PROCEEDINGS_VIEW,

                        HISTORICAL_RECORDS,

                        CITE_KEY, RECORD_KEY);

        String selectString = String.format(
                "SELECT * " + "FROM (%s) AS conf_records " + "WHERE conf_records.%s IS NOT null "
                        + "AND conf_records.%s IS NOT null",

                        innerSelect,

                        EVENT_MONTH,

                        EVENT_YEAR);

        LOGGER.debug("{}: Returning formatted command for execution:\n{}", "SQL", selectString);

        final Connection connection = this.dbm.getConnection();

        try {
            connection.setAutoCommit(false);

            stmt = connection.createStatement();

            stmt.executeQuery(selectString);
            connection.commit();

            rs = stmt.getResultSet();

            while (rs.next()) {
                Date cDate = rs.getDate(C_DATE);
                short evtMonth = rs.getShort(EVENT_MONTH);
                short evtYear = rs.getShort(EVENT_YEAR);
                long delay = rs.getLong(INSERTION_DELAY);
                String recordKey = rs.getString(CITE_KEY);
                String streamKey = rs.getString(STREAM_KEY);
                String title = rs.getString(RECORD_TITLE);
                String type = rs.getString(RECORD_TYPE);
                short year = rs.getShort(PUBLICATION_YEAR);
                String evtPlace = rs.getString(EVENT_COUNTRY);

                ConferenceRecord record = new ConferenceRecord(recordKey, streamKey, title, cDate.toLocalDate(),
                        Year.of(year), Type.fromString(type), Month.of(evtMonth), Year.of(evtYear), evtPlace, delay);

                LOGGER.debug("Read stream record: {}", record);

                boolean put = toReturn.put(streamKey, record);
                if (!put)
                    LOGGER.debug("Record has not been added to key-record map: " + record);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    LOGGER.error("Transaction is being rolled back");
                    connection.rollback();
                } catch (SQLException excep) {
                    e.printStackTrace();
                }
            }
        } finally {
            LOGGER.info(this.logFormat + " (Duration: {})", "END", "joined table",
                    Duration.between(start, Instant.now()));
            try {
                if (stmt != null) {
                    stmt.close();
                }
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return toReturn;
    }

    private Table<String, YearMonth, Double> readStreamLogScores() {
        Table<String, YearMonth, Double> streamLogScores;
        LOGGER.info(this.logFormat, "START", RAW_LOG_SCORES);
        final Instant start = Instant.now();

        streamLogScores = HashBasedTable.create();

        String selectString = String.format("SELECT * " + "FROM %s;", RAW_LOG_SCORES);

        final Connection connection = this.dbm.getConnection();

        Statement stmt = null;
        ResultSet rs = null;
        try {
            connection.setAutoCommit(false);

            stmt = connection.createStatement();

            /*
             * get all results
             */
            stmt.executeQuery(selectString);
            connection.commit();

            rs = stmt.getResultSet();

            Map<String, YearMonth> yearMonthsColumnMap = this.getYearMonthsOfLogs(rs);
            Map<String, Double> maxPerColumn = this.getMaxPerColumn(yearMonthsColumnMap.keySet());

            while (rs.next()) {

                String streamKey = rs.getString(STREAM_KEY);

                for (String columnName : yearMonthsColumnMap.keySet()) {
                    Double logScore = rs.getDouble(columnName);
                    LOGGER.debug("[{}, {}] raw log score: {}", streamKey, columnName, logScore);

                    // teilen durch max
                    logScore = logScore / maxPerColumn.get(columnName);
                    LOGGER.debug("[{}, {}] averaged log score: {}", streamKey, columnName, logScore);

                    streamLogScores.put(streamKey, yearMonthsColumnMap.get(columnName), logScore);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
            if (connection != null) {
                try {
                    LOGGER.error("Transaction is being rolled back");
                    connection.rollback();
                } catch (SQLException excep) {
                    e.printStackTrace();
                }
            }
        } finally {
            LOGGER.info(this.logFormat + " (Duration: {})", "END", RAW_LOG_SCORES,
                    Duration.between(start, Instant.now()));
            try {
                if (stmt != null) {
                    stmt.close();
                }
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return streamLogScores;
    }

    private Map<String, YearMonth> getYearMonthsOfLogs(ResultSet rs) throws SQLException {
        Map<String, YearMonth> monthsColumnMap = new HashMap<>();

        Pattern yearPattern = Pattern.compile("(?<=y)(19|20)\\d{2}(?=m)");
        Pattern monthPattern = Pattern.compile("(?<=m)\\d{2}$");

        Matcher yearMatcher = yearPattern.matcher("");
        Matcher monthMatcher = monthPattern.matcher("");

        String year, month;

        int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rs.getMetaData().getColumnName(i);
            yearMatcher.reset(columnName);
            if (yearMatcher.find()) {
                year = yearMatcher.group();
                monthMatcher.reset(columnName);
                if (monthMatcher.find()) {
                    month = monthMatcher.group();
                    YearMonth ym = YearMonth.of(Integer.parseInt(year), Integer.parseInt(month));
                    monthsColumnMap.put(columnName, ym);
                }
            }

        }
        return monthsColumnMap;
    }

    private Map<String, Double> getMaxPerColumn(Set<String> columns) throws SQLException {
        Map<String, Double> maxPerColumn = new HashMap<>();

        String selectTemplate = "SELECT MAX(%s) " + "FROM %s";

        for (String column : columns) {
            String selectMaxString = String.format(selectTemplate, column, RAW_LOG_SCORES);
            Double max = this.queryMaximum(selectMaxString);
            maxPerColumn.put(column, max);
        }

        return maxPerColumn;
    }

}
