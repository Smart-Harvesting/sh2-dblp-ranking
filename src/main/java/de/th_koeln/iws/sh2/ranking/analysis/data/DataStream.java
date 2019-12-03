package de.th_koeln.iws.sh2.ranking.analysis.data;

import java.util.Collection;

public abstract class DataStream<T> implements Comparable<DataStream<T>> {

    protected String key;
    protected Collection<T> records;

    public DataStream(String key, Collection<T> records) {
        this.key = key;
        this.records = records;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     *
     * Conference streams are compared by their keys for sorting
     */
    @Override
    public int compareTo(DataStream<T> o) {
        return this.key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof DataStream) && ((DataStream<?>) obj).key.equals(this.key);
    }

    @Override
    public int hashCode() {
        return this.key.hashCode();
    }

    @Override
    public String toString() {
        return String.format("DataStream %s with %d records.", this.key, this.records.size());
    }
}
