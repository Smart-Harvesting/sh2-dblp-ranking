package de.th_koeln.iws.sh2.ranking.analysis.evaluation;

import java.util.Comparator;
import java.util.Map;

import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceStream;


/**
 * Custom comparator to be used for sorting {@link ConferenceStream}s according
 * to their associated scores. Sorting is in descending order.
 * 
 * @author mandy
 *
 */
public class RelevanceRanker implements Comparator<ConferenceStream> {

	private Map<ConferenceStream, Double> streamScoreMap;
	private Map<ConferenceStream, Long> streamDelayMap;

	/**
	 * Constructor.
	 * 
	 * @param scores
	 *            map containing the elements to be sorted and their scores.
	 */
	public RelevanceRanker(Map<ConferenceStream, Double> scores, Map<ConferenceStream, Long> delays) {
		this.streamScoreMap = scores;
		this.streamDelayMap = delays;
	}

	@Override
	public int compare(ConferenceStream o1, ConferenceStream o2) {
		Double score1 = this.streamScoreMap.get(o1);
		Double score2 = this.streamScoreMap.get(o2);

		int compareVal = score2.compareTo(score1);
		if (compareVal == 0) {
			Long delayConf1 = this.streamDelayMap.get(o1);
			Long delayConf2 = this.streamDelayMap.get(o2);

			if(delayConf1==delayConf2) {
				return o1.getKey().compareTo(o2.getKey());
			}
			else if (null==delayConf1) {
				return -1;
			}
			else if(null==delayConf2) {
				return 1;
			}
			else return (int) (delayConf1-delayConf2);
		}
		return compareVal;
	}
}