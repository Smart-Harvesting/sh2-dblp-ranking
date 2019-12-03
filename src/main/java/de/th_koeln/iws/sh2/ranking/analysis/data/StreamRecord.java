package de.th_koeln.iws.sh2.ranking.analysis.data;

import java.time.LocalDate;
import java.time.Year;

/**
 * Java Bean class representing a generic stream record.
 *
 *
 * @author neumannm
 *
 */
public class StreamRecord implements Comparable<StreamRecord> {

    protected String streamKey;
    protected String recordKey;
    protected LocalDate cDate;
    protected String title;
    protected Year pubYear;
    protected Type type;

    public StreamRecord(String key, String streamKey, String title, LocalDate cDate, Year pubYear, Type type) {
        this.recordKey = key;
        this.streamKey = streamKey;
        this.title = title;
        this.cDate = cDate;
        this.pubYear = pubYear;
        this.type = type;
    }

    /**
     * Get the record's creation date.
     * 
     * @return the record's creation date
     */
    public LocalDate getcDate() {
        return this.cDate;
    }

    /**
     * @return the record's key
     */
    public String getRecordKey() {
        return this.recordKey;
    }

    /**
     * @return the key of the data stream this record belongs to
     */
    public String getStreamKey() {
        return this.streamKey;
    }

    public String getTitle() {
        return this.title;
    }

    public Type getType() {
        return this.type;
    }

    public Year getYear() {
        return this.pubYear;
    }

    @Override
    public int compareTo(StreamRecord o) {
        return this.recordKey.compareTo(o.recordKey);
    }

    @Override
    public String toString() {
        return String.format("Record %s (stream: %s): created on %s.", this.recordKey, this.streamKey, this.cDate);
    }
}
