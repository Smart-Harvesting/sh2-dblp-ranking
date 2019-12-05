package de.th_koeln.iws.sh2.ranking;

import static com.google.common.math.Stats.meanOf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Month;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.dblp.rbo.AmbiguousRankingException;
import org.dblp.rbo.model.IndefiniteRanking;
import org.dblp.rbo.model.SimilarityReport;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import de.th_koeln.iws.sh2.ranking.analysis.ScoreCalculator;
import de.th_koeln.iws.sh2.ranking.analysis.data.ConferenceStream;
import de.th_koeln.iws.sh2.ranking.analysis.data.util.Calculator;
import de.th_koeln.iws.sh2.ranking.analysis.data.util.EvaluationConfiguration;
import de.th_koeln.iws.sh2.ranking.analysis.data.util.EvaluationConfiguration.Builder;
import de.th_koeln.iws.sh2.ranking.analysis.data.util.ResultExporter;
import de.th_koeln.iws.sh2.ranking.analysis.evaluation.Goldstandard;
import de.th_koeln.iws.sh2.ranking.analysis.evaluation.RelevanceRanker;
import de.th_koeln.iws.sh2.ranking.config.PropertiesUtil;
import de.th_koeln.iws.sh2.ranking.core.DatabaseManager;
import de.th_koeln.iws.sh2.ranking.core.DbDataReader;

public class EvaluateApplication {
    private static Logger LOGGER = LogManager.getLogger(EvaluateApplication.class);

    private static Properties SETUP_PROPERTIES = null;
    private static Path outputFolder;
    private static Path rawOutputFolder;

    /* The evaluation year */
    // TODO make this a configurable parameter, too
    private static final int EVAL_YEAR = 2018;
    // TODO make this a configurable parameter, too
    private static final boolean SIMPLE_EVAL = true;

    private static void setRootLoggerLevel(Level level) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(level);
    }

    private static void setupOutputFolders() {
        SETUP_PROPERTIES = PropertiesUtil.loadExternalDataSetupConfig();
        outputFolder = Paths.get(SETUP_PROPERTIES.getProperty("output.basedir", "/dblp/eval/"));
        rawOutputFolder = outputFolder.resolve("raw");
        rawOutputFolder.toFile().mkdirs();
    }

    private static List<EvaluationConfiguration> setupConfigurations() {
        List<EvaluationConfiguration> configs;
        if (SIMPLE_EVAL) {
            configs = createSimpleConfigs();
        } else {
            configs = createComplexConfigs();
        }
        return configs;
    }

    private static List<Month> setupEvaluationMonths(String[] args) {
        List<Month> evalMonths = null;
        if (args.length > 0) {
            evalMonths = parseArgs(args);
        }
        if ((null == evalMonths) || (evalMonths.size() == 0)) {
            // no specific month(s) provided - take all months of the year
            LOGGER.info(
                    "No month privided or arguments could not be parsed - evaluation with all months (for which log data is available).");
            // evalMonths = Arrays.asList(Month.values());
            evalMonths = ImmutableList.of(Month.MAY, Month.JUNE, Month.JULY, Month.AUGUST, Month.SEPTEMBER,
                    Month.OCTOBER, Month.NOVEMBER);
        }
        LOGGER.info(String.format("Months for evaluation: %s", evalMonths));
        return evalMonths;
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

    public static void main(String[] args) {

        setRootLoggerLevel(Level.INFO);

        setupOutputFolders();

        final List<Month> evalMonths = setupEvaluationMonths(args);

        final Set<ConferenceStream> allConfs = getConferencesFromDatabase();
        LOGGER.info(String.format("Done reading %d conferences from database", allConfs.size()));

        final List<EvaluationConfiguration> configs = setupConfigurations();
        LOGGER.info(String.format("Done creating %d configurations for evaluation", configs.size()));

        runEvaluationPipeline(allConfs, evalMonths, configs);
    }

    private static void runEvaluationPipeline(Set<ConferenceStream> allConfs, List<Month> evalMonths,
            List<EvaluationConfiguration> configs) {

        Map<ConferenceStream, Integer> gold;

        /* create gold standard for each month */
        for (final Month month : evalMonths) {
            YearMonth evalYM = YearMonth.of(EVAL_YEAR, month);

            String targetQrelFileName = evalYM.toString() + ".qrel";
            Path targetQrelFilePath = rawOutputFolder.resolve(targetQrelFileName);

            /*
             * create the gold standard = a map of those conferences, that have already been
             * added to dblp by testMonth-testYear, to their relevance which reflects the
             * relative importance of the conference at the given point in time
             */
            gold = Goldstandard.create(allConfs, evalYM);

            ResultExporter.exportTrecQrel(gold, targetQrelFilePath);
        }

        /* go through all available configurations... */
        for (final EvaluationConfiguration config : configs) {
            Table<String, String, BigDecimal> trec_results = Tables.newCustomTable(new LinkedHashMap<>(),
                    LinkedHashMap::new);

            /* ... and evaluate each month with the current configuration */
            for (final Month month : evalMonths) {
                YearMonth evalYM = YearMonth.of(EVAL_YEAR, month);

                Path rankingFilePath = evaluate(evalYM, config, allConfs);
                runTrecEval(rawOutputFolder.resolve(evalYM + ".qrel"), rankingFilePath, trec_results);
            }

            /* average the results over all months */
            LOGGER.info("Calculating averages for config {}", config);
            calcAndAddAverages(trec_results);

            /* write results to file */
            ResultExporter.printToCsv(outputFolder.resolve(config + "RESULTS.csv"), trec_results);
            ResultExporter.exportInfo(outputFolder.resolve(config + "INFO.txt"), config);
        }

        calcRbo(evalMonths, configs);
    }

    private static void calcRbo(List<Month> evalMonths, List<EvaluationConfiguration> configs) {
        LOGGER.info(String.format("Calculating rbo... (%d months and %d configs)", evalMonths.size(), configs.size()));

        /*
         * p=0.973, d=30 p=0.992, d=100 p=0.996, d=200 p=0.9984, d=500
         */
        int depth = 500;
        double p = 0.9984;

        Table<String, String, BigDecimal> crossTable;
        /* for each month... */
        for (final Month month : evalMonths) {
            YearMonth evalYM = YearMonth.of(EVAL_YEAR, month);
            crossTable = HashBasedTable.create();
            /* look at the results of two configs each and compare rankings */
            for (final EvaluationConfiguration config : configs) {
                for (final EvaluationConfiguration otherConfig : configs) {

                    Path ranking1FilePath = rawOutputFolder.resolve(config.toString() + evalYM + ".trec");
                    Path ranking2FilePath = rawOutputFolder.resolve(otherConfig.toString() + evalYM + ".trec");

                    /*
                     * for each ranking: create IndefiniteRanking from trec-style ranking
                     */
                    IndefiniteRanking<Integer, String> ranking1 = readTrecRanking(ranking1FilePath);
                    IndefiniteRanking<Integer, String> ranking2 = readTrecRanking(ranking2FilePath);

                    /*
                     * create SimilarityReport from the two rankings
                     */
                    SimilarityReport<Integer, String> report = ranking1.compareTo(ranking2);
                    double rboExt = report.rboExt(p, depth);
                    crossTable.put(config.toString(), otherConfig.toString(), BigDecimal.valueOf(rboExt));
                }
            }
            ResultExporter.printToCsv(outputFolder.resolve(month + "_RBO.csv"), crossTable);
        }

        LOGGER.info("Done calculating rbo.");
    }

    /**
     *
     * Read the ranking back from a file in TREC format.
     *
     * @param path
     *            name of the file - file has to be in TREC format
     * @return instance of {@link IndefiniteRanking}
     */
    private static IndefiniteRanking<Integer, String> readTrecRanking(Path path) {
        IndefiniteRanking<Integer, String> result = null;

        try {
            Multimap<Integer, String> ranking = ArrayListMultimap.create();

            Files.lines(path).forEach(line -> {
                String[] lineParts = line.split("\t");
                @Nullable
                Integer rank = Integer.parseInt(lineParts[3]) + 1;
                @Nullable
                String conf = lineParts[2];
                ranking.put(rank, conf);
            });

            result = IndefiniteRanking.of(ranking);
        } catch (IOException e) {
            LOGGER.error(String.format("Error reading file '%s'", path), e);
            return null;
        } catch (AmbiguousRankingException e) {
            LOGGER.error("Ranking contains duplicates");
            return null;
        }
        return result;
    }

    /**
     * Evaluate the ranking for a given month.
     *
     * @param evalYM
     *            month to evaluate
     * @param config
     *            configuration to use
     * @param confs
     *            set of all conferences to rank
     * @param rbo_results
     * @return
     */
    private static Path evaluate(YearMonth evalYM, EvaluationConfiguration config, Set<ConferenceStream> confs) {
        Map<ConferenceStream, Double> scored;

        String targetRunFileName = config.toString() + evalYM.toString() + ".trec";

        Path targetRunFilePath = rawOutputFolder.resolve(targetRunFileName);

        LOGGER.info("Evaluating month " + evalYM.getMonth() + " of " + EVAL_YEAR);

        ScoreCalculator scorer = new ScoreCalculator(config);
        scored = new TreeMap<>();

        /*
         * Go through _all_ conferences and score each one
         */
        for (ConferenceStream conf : confs) {
            Double score = scorer.getScore(conf, evalYM);
            if (score != null)
                scored.put(conf, score);
        }

        // create a mapping of streams to delays to be used for tie-breaking when
        // sorting result list
        Map<ConferenceStream, Long> delays = calculateDelays(confs, evalYM);
        // for (ConferenceStream key : delays.keySet().stream()
        // .collect(ImmutableSortedSet.toImmutableSortedSet(Comparator.naturalOrder())))
        // {
        // LOGGER.info(String.format("[%s, %s] delay: %d", evalYM, key.getKey(),
        // delays.get(key)));
        // }
        /*
         *
         */
        /*
         * Get a sorted list of the conferences, sorted by their score in descending
         * order.
         *
         * Sorting is not needed for the trec evaluation, just for manual inspection of
         * ranked lists and rbo calculations.
         */
        ArrayList<ConferenceStream> ranked = new ArrayList<>(scored.keySet());
        Collections.sort(ranked, new RelevanceRanker(Collections.unmodifiableMap(scored), delays));

        ResultExporter.exportTrecResult(ranked, scored, targetRunFilePath, evalYM.toString());

        return targetRunFilePath;
    }

    private static Map<ConferenceStream, Long> calculateDelays(Collection<ConferenceStream> conferenceStreams,
            YearMonth evalYM) {

        Map<ConferenceStream, Long> delays = new HashMap<>();

        for (ConferenceStream conferenceStream : conferenceStreams) {
            Optional<Long> monthsSinceLastCreation = Calculator
                    .calcMonthsSinceLastCreation(conferenceStream.getRecordsCreatedBefore(evalYM), evalYM);
            if (monthsSinceLastCreation.isPresent()) {
                delays.put(conferenceStream, monthsSinceLastCreation.get());
            }
        }
        return delays;
    }

    /**
     * Run the trec_eval script on the result.
     *
     * @param targetQrelFilePath
     *            path to the qrels file
     * @param targetRunFilePath
     *            path to the run file
     * @param results
     *            table where the results go to
     */
    private static void runTrecEval(Path targetQrelFilePath, Path targetRunFilePath,
            Table<String, String, BigDecimal> results) {
        String scriptPath = EvaluateApplication.class.getClassLoader().getResource("script/trec_eval").getPath();
        LOGGER.info("Found trec-eval script in path: " + scriptPath);

        List<String> command = new ArrayList<>();
        command.add(scriptPath);
        command.add("-m");
        command.add("all_trec");
        command.add(targetQrelFilePath.toString());
        command.add(targetRunFilePath.toString());
        ProcessBuilder pb = new ProcessBuilder(command);

        try {
            String line = null;
            BigDecimal value;
            String measurement;

            Process p = pb.start();

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

            // read first line to use as header later on
            line = stdInput.readLine();
            String month = line.split("\t")[2].trim();
            // read the output from the command
            while ((line = stdInput.readLine()) != null) {
                // type of measurement in first column
                measurement = line.split("\t")[0].trim();
                try {
                    // value in third column
                    value = new BigDecimal(line.split("\t")[2].trim());

                    results.put(measurement, month, value);
                } catch (NumberFormatException nfe) {
                    LOGGER.warn(String.format("Could not parse double value from trec result. Line: '%s'", line));
                    continue;// TODO OK? - should not happen
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * For each row in the result table, calculate the average of the row's values.
     * Add the calculated values as a new column to the table.
     *
     * @param results
     *            result table
     */
    private static void calcAndAddAverages(Table<String, String, BigDecimal> results) {
        Set<String> rowKeySet = results.rowKeySet();
        for (String rowKey : rowKeySet) {
            List<BigDecimal> values = new ArrayList<>(results.row(rowKey).values());
            LOGGER.debug("{} values: {}", rowKey, values);
            double avg = meanOf(values);
            LOGGER.debug("{} mean (google): {}", rowKey, avg);
            results.put(rowKey, "AVG", BigDecimal.valueOf(avg));
        }
    }

    /**
     * Parse the command line arguments.
     *
     * @param args
     * @return month given for evaluation
     */
    private static List<Month> parseArgs(String[] args) {
        List<Month> monthArgs = new ArrayList<>();

        for (String arg : args) {
            try {
                Month parsedMonthArg = Month.valueOf(arg.toUpperCase());
                LOGGER.debug("Command line argument has been parsed to " + parsedMonthArg);
                monthArgs.add(parsedMonthArg);
            } catch (IllegalArgumentException iae) {
                LOGGER.fatal(
                        "Unable to parse argument(s) as month name(s). Did you provide the argument(s) in English?",
                        iae);
                continue;
            }
        }
        return monthArgs;
    }

    /**
     * create a list of @link{EvaluationConfiguration} objects with different
     * configurations.
     *
     * Until now only base configs (1 factor at a time, no combinations).
     *
     * @return list of {@link EvaluationConfiguration} objects with different
     *         configurations
     */
    private static List<EvaluationConfiguration> createSimpleConfigs() {
        List<EvaluationConfiguration> toReturn = new ArrayList<>();

        Builder builder = new EvaluationConfiguration.Builder();
        toReturn.add(builder.build());

        builder = new EvaluationConfiguration.Builder().useCitationScore();
        toReturn.add(builder.build());

        builder = new EvaluationConfiguration.Builder().useActivityScore();
        toReturn.add(builder.build());

        builder = new EvaluationConfiguration.Builder().useSizeScore();
        toReturn.add(builder.build());

        builder = new EvaluationConfiguration.Builder().useIntlScore();
        toReturn.add(builder.build());

        builder = new EvaluationConfiguration.Builder().usePromScore();
        toReturn.add(builder.build());

        builder = new EvaluationConfiguration.Builder().useRatingScore();
        toReturn.add(builder.build());

        builder = new EvaluationConfiguration.Builder().useAffilScore();
        toReturn.add(builder.build());

        builder = new EvaluationConfiguration.Builder().useLogScore();
        toReturn.add(builder.build());

        return toReturn;
    }

    /**
     * create a list of @link{EvaluationConfiguration} objects with different
     * configurations.
     *
     * Configs are complex, 2 factors at a time.
     *
     * @return list of {@link EvaluationConfiguration} objects with different
     *         configurations
     */
    private static List<EvaluationConfiguration> createComplexConfigs() {
        List<EvaluationConfiguration> toReturn = new ArrayList<>();
        Builder builder;

        // Ergebnis der Signifikanztests
        // rating + activity
        builder = new EvaluationConfiguration.Builder().useActivityScore().useRatingScore();
        toReturn.add(builder.build());

        // rating + prominence
        builder = new EvaluationConfiguration.Builder().usePromScore().useRatingScore();
        toReturn.add(builder.build());

        // activity + prominence
        builder = new EvaluationConfiguration.Builder().useActivityScore().usePromScore();
        toReturn.add(builder.build());

        // activity+rating+prominence
        builder = new EvaluationConfiguration.Builder().useActivityScore().useRatingScore().usePromScore();
        toReturn.add(builder.build());

        return toReturn;
    }

}
