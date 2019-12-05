package de.th_koeln.iws.sh2.ranking.core;

import java.util.Set;

import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceStream;

public interface DataReader {

    public Set<ConferenceStream> getData();
}
