package de.th_koeln.iws.sh2.ranking.analysis.evaluation;

import java.time.Year;
import java.time.YearMonth;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.TreeMultimap;

import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceStream;
import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceRecord;
import de.th_koeln.iws.sh2.ranking.analysis.data.util.Calculator;


/**
 * Baseline for evaluation
 * 
 * @author mandy
 *
 */
public final class Goldstandard {

	@SuppressWarnings("unused")
	private static Logger LOGGER = LogManager.getLogger(Goldstandard.class);

	private Goldstandard() {
	} // enforce non-instantiability

	/**
	 * Create the gold standard from a given year and month.
	 * 
	 * @param allConfs
	 *            - all conferences
	 * @param evalYear
	 *            - the test year, e.g. 2016
	 * @param evalMonth
	 *            - the text month, e.g. SEPTEMBER
	 * @return a list of conference keys that are relevant at the given point in
	 *         time
	 */
	public static Map<ConferenceStream, Integer> create(Set<ConferenceStream> allConfs, YearMonth evalYM) {
		Map<ConferenceStream, Integer> result = new TreeMap<>();

		/*
		 * how to create:
		 * 
		 * go through all conferences
		 * 
		 * determine the latest record where the conference took place before the test
		 * year
		 * 
		 * check if cDate of this record is before the test month in test year (if
		 * latest record had already been added to dblp)
		 * 
		 * if yes, tetermine relevance score
		 * 
		 */
		for (ConferenceStream stream : allConfs) {

			int relevance = defineRelevance(stream, evalYM);

			result.put(stream, relevance);

		}
		return result;
	}

	/**
	 * A conference streams' relevance is defined: Take all records that have been
	 * included until (including) the evaluation year. Check if the creation date of
	 * the latest event record has already passed. If not, the stream is not yet
	 * relevant at that given point in time. If yes, the relevance is defined
	 * depending on how many months have passed since creation.
	 *
	 * @param stream
	 *            the stream to define the relevance of
	 * @param evalYM
	 *            the point in time in relation to which the relevance shall be
	 *            defined
	 * @return
	 */
	private static int defineRelevance(ConferenceStream stream, YearMonth evalYM) {
		TreeMultimap<YearMonth, ConferenceRecord> recordsCreated = stream
				.getRecordsCreatedInYear(Year.of(evalYM.getYear()));
		Optional<Long> monthsSinceLastCreation = Calculator.calcMonthsSinceLastCreation(recordsCreated, evalYM);
		if (monthsSinceLastCreation.isPresent()) {
			long monthsSinceCreation = monthsSinceLastCreation.get();
			/*
			 * interval-based relevance:
			 *
			 * if the record has been added in the last month, it is highly relevant
			 *
			 * if it has been added between 1 and 2 months before, it is very relevant
			 *
			 * if it has been added between 3 and 5 months before, it is relevant
			 *
			 * if it has been added 6 months or more before, it is just a little relevant
			 */
			if (isInBetween(monthsSinceCreation, 0, 0))
				return 4;
			if (isInBetween(monthsSinceCreation, 1, 2))
				return 3;
			if (isInBetween(monthsSinceCreation, 3, 5))
				return 2;
			if (monthsSinceCreation >= 6)
				return 1;
		} else {
			return 0;
		}
		return 0;
	}

	private static boolean isInBetween(long x, int lower, int upper) {
		return (lower <= x) && (x <= upper);
	}
}
