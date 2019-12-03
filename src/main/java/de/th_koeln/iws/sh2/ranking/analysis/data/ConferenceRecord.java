package de.th_koeln.iws.sh2.ranking.analysis.data;

import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;

/**
 * Java Bean class representing a conference stream record.
 *
 *
 * @author neumannm
 *
 */
public class ConferenceRecord extends StreamRecord{

    // conference-specific fields: event dates and place
    private Month evtMonth;
    private Year evtYear;
    private String evtPlace;
    private long insertDelayInMonths;

    public ConferenceRecord(String key, String streamKey, String title, LocalDate cDate, Year pubYear, Type type,
            Month eventMonth, Year eventYear, String eventPlace, long insertDelay) {
        super(key, streamKey, title, cDate, pubYear, type);
        this.evtMonth = eventMonth;
        this.evtYear = eventYear;
        this.evtPlace = eventPlace;
        this.insertDelayInMonths = insertDelay;
    }

    public Month getEvtMonth() {
        return this.evtMonth;
    }

    public Year getEvtYear() {
        return this.evtYear;
    }

    public String getEvtPlace() {
        return this.evtPlace;
    }

    public YearMonth getEvtYearMonth() {
        return YearMonth.of(this.evtYear.getValue(), this.evtMonth.getValue());
    }

    public long getInsertDelayInMonths() {
        return this.insertDelayInMonths;
    }

    @Override
    public String toString() {
        return String.format("Conference record %s (stream: %s): created on %s, event was on %s, delay of %d",
                this.recordKey, this.streamKey, this.cDate,
                YearMonth.of(this.evtYear.getValue(), this.evtMonth).toString(), this.insertDelayInMonths);
    }
}