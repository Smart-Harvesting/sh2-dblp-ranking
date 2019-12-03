package de.th_koeln.iws.sh2.ranking.analysis.data;

import static java.util.stream.Collectors.toList;

import java.time.Year;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

import de.th_koeln.iws.sh2.ranking.analysis.data.util.RecordByCDateComparator;

/**
 * Class that represents a conference stream and bundles all the records
 * belonging to this stream.
 *
 * @author neumannm
 *
 */
public class ConferenceStream extends DataStream<ConferenceRecord> {

    private ListMultimap<Year, ConferenceRecord> recordsByEvtYear;

    private Double avgRating, intlScore, citScore, promScore, sizeScore, affilScore;

    Map<YearMonth, Double> logScores;

    /**
     * Constructor.
     *
     * @param streamKey
     *            the key of the conference stream
     * @param records
     *            all records belonging to a conference (i.e. proceedings)
     */
    public ConferenceStream(String streamKey, Collection<ConferenceRecord> records) {
        super(streamKey, records);
        this.createRecordsByEvtYearMap();
    }

    public ConferenceStream(String streamKey) {
        this(streamKey, new ArrayList<ConferenceRecord>());
    }

    private void createRecordsByEvtYearMap() {
        // build a Multimap that orders Keys (years) in descending order
        this.recordsByEvtYear = MultimapBuilder.treeKeys(DESC_ORDER).arrayListValues().build();
        // füge alle event years der einzelnen records und die records selbst zur
        // Multimap hinzu
        this.records.forEach(r -> this.recordsByEvtYear.put(r.getEvtYear(), r));
    }

    static final Comparator<Year> DESC_ORDER = new Comparator<Year>() {
        @Override
        public int compare(Year e1, Year e2) {
            return e2.compareTo(e1);
        }
    };

    /**
     * Get all records with events before a given year, sorted in reverse order by
     * event year.
     *
     * @param year
     * @return all records of events that are from before the given year, sorted in
     *         reverse order (newest record first)
     */
    public TreeMultimap<YearMonth, ConferenceRecord> getRecordsWithEventsBefore(Year year) {
        Predicate<? super ConferenceRecord> predicate = r -> r.getEvtYear().isBefore(year);
        Ordering<YearMonth> keyOrdering = Ordering.natural().reverse();
        Ordering<ConferenceRecord> valueOrdering = Ordering.natural();
        return this.filterAndMapEventRecords(predicate, keyOrdering, valueOrdering);
    }

    /**
     * Get all records with events before a given year-month, sorted in reverse
     * order by event year.
     *
     * @param pubYear
     * @return all records of events that are from before the given year-month,
     *         sorted in reverse order (newest record first)
     */
    public TreeMultimap<YearMonth, ConferenceRecord> getRecordsWithEventsBefore(YearMonth yearMonth) {
        Predicate<? super ConferenceRecord> predicate = r -> r.getEvtYearMonth().isBefore(yearMonth);
        Ordering<YearMonth> keyOrdering = Ordering.natural().reverse();
        Ordering<ConferenceRecord> valueOrdering = Ordering.natural();
        return this.filterAndMapEventRecords(predicate, keyOrdering, valueOrdering);
    }

    /**
     * Get all records created before a given year, sorted in reverse order by event
     * year.
     *
     * @param year
     * @return all records that are created before the given year, sorted in reverse
     *         order (newest record first). Values for each key are sorted by
     *         creation date in reverse order (latest first). Values with same
     *         creation date are sorted alphabetically.
     */
    public TreeMultimap<YearMonth, ConferenceRecord> getRecordsCreatedBefore(Year year) {
        Predicate<? super ConferenceRecord> predicate = r -> Year.from(r.getcDate()).isBefore(year);
        Ordering<YearMonth> keyOrdering = Ordering.natural().reverse();
        Ordering<ConferenceRecord> valueOrdering = Ordering.from((r2, r1) -> new RecordByCDateComparator().compare(r2, r1));
        return this.filterAndMapEventRecords(predicate, keyOrdering, valueOrdering);
    }

    /**
     * Get all records created up until and including a given year, sorted in
     * reverse order by event year.
     *
     * @param year
     * @return all records that are created up until and including the given year,
     *         sorted in reverse order (newest record first). Values for each key
     *         are sorted by creation date. Values with same creation date are
     *         sorted alphabetically.
     */
    public TreeMultimap<YearMonth, ConferenceRecord> getRecordsCreatedUntilIncludingYear(Year year) {
        Predicate<? super ConferenceRecord> predicate = r -> !Year.from(r.getcDate()).isAfter(year);
        Ordering<YearMonth> keyOrdering = Ordering.natural().reverse();
        Ordering<ConferenceRecord> valueOrdering = Ordering.from((r1, r2) -> new RecordByCDateComparator().compare(r1, r2));
        return this.filterAndMapEventRecords(predicate, keyOrdering, valueOrdering);
    }

    /**
     * Get all records created before a given year-month, sorted in reverse order by
     * event year.
     *
     * @param pubYear
     * @return all records that are created before the given year-month, sorted in
     *         reverse order (newest record first). Values with same creation date
     *         are sorted alphabetically.
     */
    public TreeMultimap<YearMonth, ConferenceRecord> getRecordsCreatedBefore(YearMonth yearMonth) {
        Predicate<? super ConferenceRecord> predicate = r -> YearMonth.from(r.getcDate()).isBefore(yearMonth);
        Ordering<YearMonth> keyOrdering = Ordering.natural().reverse();
        Ordering<ConferenceRecord> valueOrdering = Ordering.from((r2, r1) -> new RecordByCDateComparator().compare(r2, r1));
        return this.filterAndMapEventRecords(predicate, keyOrdering, valueOrdering);
    }

    public TreeMultimap<YearMonth, ConferenceRecord> getRecordsCreatedInYear(Year year) {
        Predicate<? super ConferenceRecord> predicate = r -> r.getcDate().getYear() == year.getValue();
        Ordering<YearMonth> keyOrdering = Ordering.natural().reverse();
        Ordering<ConferenceRecord> valueOrdering = Ordering.from((r2, r1) -> new RecordByCDateComparator().compare(r2, r1));
        return this.filterAndMapEventRecords(predicate, keyOrdering, valueOrdering);
    }

    private TreeMultimap<YearMonth, ConferenceRecord> filterAndMapEventRecords(Predicate<? super ConferenceRecord> predicate,
            Comparator<YearMonth> keyOrdering, Comparator<ConferenceRecord> valueOrdering) {

        TreeMultimap<YearMonth, ConferenceRecord> eventRecordMapping = TreeMultimap.create(keyOrdering, valueOrdering);

        List<ConferenceRecord> filteredRecords = this.records.stream().filter(predicate).collect(toList());

        for (ConferenceRecord conferenceRecord : filteredRecords) {
            eventRecordMapping.put(conferenceRecord.getEvtYearMonth(), conferenceRecord);
        }
        return eventRecordMapping;
    }

    /**
     * @return all records of this conference stream
     */
    public Collection<ConferenceRecord> getRecords() {
        return this.records;
    }

    /**
     * @return conference stream key
     */
    public String getKey() {
        return this.key;
    }

    /**
     * Get all the records of this conference stream by their event year. The years
     * are sorted in descending order.
     *
     * @return a mapping from event years to stream records
     */
    public ListMultimap<Year, ConferenceRecord> getRecordsByEventYear() {
        return this.recordsByEvtYear;
    }

    /**
     * Get the averge rating of a conference
     *
     * 0 == D
     *
     * 1 == C
     *
     * 2 == B
     *
     * 3 == A
     *
     * 4 == A*
     *
     * Values are the mean of all available ratings for this conference (as they may
     * change over the years).
     *
     * @return the average rating
     */
    public Double getAvgRating() {
        return this.avgRating;
    }

    public void setRatingScore(Double avgRating) {
        this.avgRating = avgRating;
    }

    public void setIntlScore(Double intlScore) {
        this.intlScore = intlScore;
    }

    public void setPromScore(Double promScore) {
        this.promScore = promScore;
    }

    public void setCitScore(Double citScore) {
        this.citScore = citScore;
    }

    public void setSizeScore(Double sizeScore) {
        this.sizeScore = sizeScore;
    }

    public Double getIntlScore() {
        if (null == this.intlScore)
            return 0.0;
        return this.intlScore;
    }

    public Double getCitationsScore() {
        if (null == this.citScore)
            return 0.0;
        return this.citScore;
    }

    public Double getProminence() {
        if (null == this.promScore)
            return 0.0;
        return this.promScore;
    }

    public Double getAvgSize() {
        if (null == this.sizeScore)
            return 0.0;
        return this.sizeScore;
    }

    public Double getAffilScore() {
        return this.affilScore;
    }

    public Map<YearMonth, Double> getLogScores() {
        return this.logScores;
    }

    public void setAffilScore(Double affilScore) {
        this.affilScore = affilScore;
    }

    public void setLogScores(Map<YearMonth, Double> map) {
        this.logScores = map;
    }

}