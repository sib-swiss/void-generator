package swiss.sib.swissprot.servicedescription;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import swiss.sib.swissprot.servicedescription.io.ServiceDescriptionRDFWriter;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.Counters;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import swiss.sib.swissprot.voidcounter.Variables;
import swiss.sib.swissprot.voidcounter.sparql.SparqlCounters;
import swiss.sib.swissprot.voidcounter.virtuoso.VirtuosoCounters;
import virtuoso.rdf4j.driver.VirtuosoRepository;

@Command(name = "void-generate", mixinStandardHelpOptions = true, description = "Generate a void file", versionProvider = GitPropertiesVersionFetch.class)
public class Generate implements Callable<Integer> {
	private static final Logger log = LoggerFactory.getLogger(Generate.class);
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	private static final Pattern COMMA = Pattern.compile(",", Pattern.LITERAL);
	
	@Option(names = "--find-distinct-classes", defaultValue = "true", description = "Setting this to false will disable the search for distinct classes. And also Linksets and limits most use cases")
	private boolean findDistinctClasses = true;
	@Option(names = "--count-distinct-subjects", defaultValue = "true")
	private boolean countDistinctSubjects = true;
	@Option(names = "--count-distinct-objects", defaultValue = "true")
	private boolean countDistinctObjects = true;
	@Option(names = "--find-predicates", defaultValue = "true")
	private boolean findPredicates = true;
	@Option(names = "--count-detailed-void-statistics", defaultValue = "true")
	private boolean detailedCount = true;

	@Option(names = "--ontology-graph-name", description = "ontology graph name (can be added multiple times")
	List<String> ontologyGraphNames;

	@Option(names = { "-s", "--void-file" }, required = true)
	private File sdFile;


	@Option(names = { "--virtuoso-jdbc" }, description = "A virtuoso jdbc connection string")
	private String virtuosoJdcb;

	@Option(names = { "-r", "--repository" }, description = "A SPARQL http/https endpoint location")
	private String repositoryLocator;

	@Option(names = { "-f",
			"--force" }, description = "Force regeneration of the void file discarding previously provided data")
	private boolean forcedRefresh;

	@Option(names = { "-i",
			"--iri-of-void" }, description = "The base IRI where this void file should reside", required = true)
	private String iriOfVoidAsString;
	private IRI iriOfVoid;
	private Set<IRI> knownPredicates;

	@Option(names = { "-g", "--graphs" }, description = "The IRIs of the graphs to query.")
	private String commaSeperatedGraphs;

	@Option(names = { "-p", "--predicates" }, description = "Commas separated IRIs of the predicates in to be counted.")
	private String commaSeperatedKnownPredicates;

	@Option(names = { "--user" }, description = "Virtuoso user", defaultValue = "dba")
	private String user;

	@Option(names = { "--password" }, description = "Virtuoso password", defaultValue = "dba")
	private String password;

	@Option(names = { "--add-void-graph-to-store",
			"-a" }, description = "immediatly attempt to store the void graph into the store and add it to the total count", defaultValue = "false")
	private boolean addResultsToStore = false;

	@Option(names = {
			"--max-concurrency", }, description = "issue no more than this number of queries at the same time")
	private int maxConcurrency = 0;

	@Option(names = { "--from-test-file" }, description = "generate a void/service description for a test file")
	private File fromTestFile;

	@Option(names = {
			"--filter-expression-to-exclude-classes-from-void" }, description = "Some classes are not interesting for the void file, as they are to rare. Can occur if many classes have instances but the classes do not represent a schema as such. Variable should be '?clazz'")
	private String classExclusion;

	@Option(names = {
			"--data-release-version" }, description = "Set a 'version' for the sparql-endpoint data and the datasets")
	private String dataVersion;

	@Option(names = {
			"--data-release-date" }, description = "Set a 'date' of release for the sparql-endpoint data and the datasets")
	private String dataReleaseDate;

	@Option(names = {
			"--optimize-for" }, description = "Which store to optimize for. Virtuoso, Qlever or a SPARQLunion (all triples are in named graphs and the default graph is the union of all named graphs", defaultValue = "sparql")
	private String optimizeFor = "sparql";
	
	@Option(names = {"-V", "--version"}, versionHelp = true, description = "display version info")
	boolean versionInfoRequested;
	
	@Option(names = {"?", "-h", "--help"}, usageHelp = true, description = "display this help message")
	boolean usageHelpRequested;

	public static void main(String[] args) {
		var cl = new CommandLine(new Generate());
		if (cl.isUsageHelpRequested()) {
			cl.usage(System.out);
		} else if (cl.isVersionHelpRequested()) {
			cl.printVersionHelp(System.out);
		} else if (args.length == 0) {
			cl.usage(System.out);
			System.exit(1);
		} else {
			int exitCode = cl.execute(args);
			System.exit(exitCode);
		}
	}

	private final ServiceDescription sd;
	private final AtomicInteger scheduledQueries = new AtomicInteger();
	private final AtomicInteger finishedQueries = new AtomicInteger();
	private Set<String> graphNames = Set.of();
	private Repository repository;
	private Counters counters;



	@Override
	public Integer call() {
		if (commaSeperatedGraphs != null)
			this.graphNames = COMMA.splitAsStream(commaSeperatedGraphs).collect(Collectors.toSet());
		else
			this.graphNames = new HashSet<>();

		if (log.isDebugEnabled()) {
			log.debug("Void listener for {}", graphNames.stream().collect(Collectors.joining(", ")));
		}
		if (commaSeperatedKnownPredicates != null) {
			this.knownPredicates = COMMA.splitAsStream(commaSeperatedKnownPredicates).map(VF::createIRI)
					.collect(Collectors.toSet());
		} else
			this.knownPredicates = new HashSet<>();

		if (virtuosoJdcb != null) {
			repository = new VirtuosoRepository(virtuosoJdcb, user, password);
		} else if (fromTestFile != null) {
			MemoryStore ms = new MemoryStore();
			ms.init();

			repository = new SailRepository(ms);
			repository.init();
			try (RepositoryConnection conn = repository.getConnection()) {
				IRI graph = conn.getValueFactory().createIRI(fromTestFile.toURI().toString());
				conn.begin();
				conn.add(fromTestFile, graph);
				conn.commit();
			} catch (RDFParseException | RepositoryException | IOException e) {
				return 1;
			}
		} else if (repositoryLocator.startsWith("http")) {
			SPARQLRepository sr = new SPARQLRepository(repositoryLocator);
			sr.enableQuadMode(true);
			sr.setAdditionalHttpHeaders(Map.of("User-Agent", "void-generator"));
			HttpClientBuilder hcb = HttpClientBuilder.create();
			hcb.setRedirectStrategy(LaxRedirectStrategy.INSTANCE);
			hcb.setMaxConnPerRoute(maxConcurrency).setMaxConnTotal(maxConcurrency).setUserAgent("void-generator-robot");
			sr.setHttpClient(hcb.build());
			repository = sr;
		}
		var update = update();
		// Virtuoso was not started by us so we should not send it a shutdown command
		if (virtuosoJdcb == null) {
			repository.shutDown();
		}
		if (update > 0) {
			log.error("There were {} queries that did not finish", update);
			return update;
		} else {
			return 0;
		}
	}

	public Generate() {
		super();
		this.sd = new ServiceDescription();
		// At least 1 but no more than one third of the cpus
	}

	private final List<Future<Exception>> futures = Collections.synchronizedList(new ArrayList<>());
	private final List<QueryCallable<?, ? extends Variables>> tasks = Collections.synchronizedList(new ArrayList<>());

	private ExecutorService executor;
	private Executor delayedExecutor;
	private Semaphore limit;

	private CompletableFuture<Exception> findingGraphs = null;

	private final CompletableFuture<Exception> schedule(QueryCallable<?, ? extends Variables> task) {
		scheduledQueries.incrementAndGet();
		tasks.add(task);
		CompletableFuture<Exception> cf = CompletableFuture.supplyAsync(task::call, executor);
		futures.add(cf);
		return cf;
	}

	private final CompletableFuture<Exception> scheduleAgain(QueryCallable<?, ? extends Variables> task) {
		scheduledQueries.incrementAndGet();
		tasks.add(task);
		CompletableFuture<Exception> cf = CompletableFuture.supplyAsync(task::call, delayedExecutor);
		futures.add(cf);
		return cf;
	}

	public int update() {
		if (log.isDebugEnabled())
			log.debug("Void listener for {}", graphNames.stream().collect(Collectors.joining(", ")));
		if (dataReleaseDate != null) {
			sd.setReleaseDate(LocalDate.from(DateTimeFormatter.ISO_DATE.parse(dataReleaseDate)));
		}
		sd.setVersion(dataVersion);
		if (commaSeperatedKnownPredicates != null) {
			this.knownPredicates = COMMA.splitAsStream(commaSeperatedKnownPredicates).map(VF::createIRI)
					.collect(Collectors.toSet());
		} else
			this.knownPredicates = new HashSet<>();
		this.iriOfVoid = SimpleValueFactory.getInstance().createIRI(iriOfVoidAsString);
		if (maxConcurrency <= 0) {
			maxConcurrency = Math.max(1, Runtime.getRuntime().availableProcessors() / 3);
		}
		limit = new Semaphore(maxConcurrency);
		executor = Executors.newFixedThreadPool(maxConcurrency);
		delayedExecutor = CompletableFuture.delayedExecutor(1, TimeUnit.MINUTES, executor);
		ReadWriteLock rwLock = new ReentrantReadWriteLock();
		Consumer<ServiceDescription> saver = createSaver(rwLock);
		if (repository instanceof VirtuosoRepository) {
			optimizeFor = "virtuoso";
		}
		log.info("Optimizing for backing store: {}", optimizeFor);

		CommonVariables cv = scheduleCounters(sd, saver, rwLock);

		int res = waitForCountToFinish(futures);
		if (addResultsToStore) {
			log.debug("Starting the count of the void data itself using {}", maxConcurrency);
			countTheVoidDataItself(iriOfVoid, cv, rwLock);
			res += waitForCountToFinish(futures);
			saveResults(iriOfVoid, saver);

			log.debug("Starting the count of the void data itself a second time using {}", maxConcurrency);

			countTheVoidDataItself(iriOfVoid, cv, rwLock);
			res += waitForCountToFinish(futures);
		}
		saveResults(iriOfVoid, saver);
		executor.shutdown();
		return res;
	}

	private Consumer<ServiceDescription> createSaver(ReadWriteLock rwLock) {
		Consumer<ServiceDescription> saver;
		if (repository instanceof VirtuosoRepository) {
			File distinctSubjectIrisFile = new File(sdFile.getAbsolutePath() + ".distinct-subjects");
			File distinctObjectIrisFile = new File(sdFile.getAbsolutePath() + ".distinct-objects");
			ConcurrentMap<String, Roaring64NavigableMap> distinctSubjectIris = VirtuosoCounters.readGraphsWithSerializedBitMaps(
					distinctSubjectIrisFile, forcedRefresh);
			ConcurrentMap<String, Roaring64NavigableMap> distinctObjectIris = VirtuosoCounters.readGraphsWithSerializedBitMaps(
					distinctObjectIrisFile, forcedRefresh);
			var vcounters = new VirtuosoCounters(distinctSubjectIris, distinctObjectIris, this::schedule);
			this.counters = vcounters;
			saver = sdg -> {
				writeServiceDescription(sdg, iriOfVoid, rwLock);
				vcounters.writeGraphs(distinctSubjectIrisFile, distinctObjectIrisFile, rwLock);
			};
		} else {
			OptimizeFor fromString = OptimizeFor.fromString(optimizeFor);
			this.counters = new SparqlCounters(fromString, this::schedule, this::scheduleAgain);
			saver = sdg -> writeServiceDescription(sdg, iriOfVoid, rwLock);
		}
		return saver;
	}

	private void writeServiceDescription(ServiceDescription sdg, IRI iriOfVoid, ReadWriteLock rwLock) {
		final Lock readLock = rwLock.readLock();
		try {
			readLock.lock();
			Optional<RDFFormat> f = Rio.getWriterFormatForFileName(sdFile.getName());
			try (OutputStream os = new FileOutputStream(sdFile)) {
				ServiceDescriptionRDFWriter.write(sdg, iriOfVoid, f.orElseGet(() -> RDFFormat.TURTLE), os,
						VF.createIRI(repositoryLocator));
			} catch (Exception e) {
				log.error("can not store ServiceDescription", e);
			}
		} finally {
			readLock.unlock();
		}
	}

	private void saveResults(IRI graphUri, Consumer<ServiceDescription> saver) {
		sd.setTotalTripleCount(sd.getGraphs().stream().mapToLong(GraphDescription::getTripleCount).sum());
		saver.accept(sd);
		if (addResultsToStore) {
			try (RepositoryConnection connection = repository.getConnection()) {
				clearGraph(connection, graphUri);
				updateGraph(connection, graphUri);
			} catch (RepositoryException | MalformedQueryException | IOException e) {
				log.error("Generating stats for SD failed", e);
			}
		}
	}

	private void countTheVoidDataItself(IRI voidGraph, CommonVariables cv, ReadWriteLock rwLock) {
		String voidGraphUri = voidGraph.toString();
		GraphDescription voidGraphDescription = getOrCreateGraphDescriptionObject(voidGraphUri, sd, rwLock);
		counters.countSpecifics(cv, voidGraphDescription, countDistinctSubjects, countDistinctObjects);
		counters.findPredicatesAndClasses(cv, knownPredicates, voidGraphDescription, findDistinctClasses,
				findPredicates, detailedCount, classExclusion);
	}

	private int waitForCountToFinish(List<Future<Exception>> futures) {
		try {
			int loop = 0;
			while (!futures.isEmpty()) {
				final int last = futures.size() - 1;
				final Future<Exception> next = futures.get(last);
				if (next.isCancelled())
					log.error("{} was cancelled", next);
				else if (next.isDone())
					futures.remove(last);
				else {
					log.info("Queries " + finishedQueries.get() + "/" + scheduledQueries.get());
					loop = checkProgress(futures, loop, last, next);
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		if (finishedQueries.get() == scheduledQueries.get()) {
			log.info("Ran " + finishedQueries.get() + " queries");
		} else if (finishedQueries.get() < scheduledQueries.get()) {
			log.error("Scheduled more queries than finished: " + finishedQueries.get() + "/" + scheduledQueries.get());
		} else {
			log.error("Finished more queries than scheduled: " + finishedQueries.get() + "/" + scheduledQueries.get());
		}
		return scheduledQueries.get() - finishedQueries.get();
	}

	private int checkProgress(List<Future<Exception>> futures, int loop, final int last, final Future<Exception> next)
			throws InterruptedException {
		try {
			Exception exception = next.get(1, TimeUnit.MINUTES);
			futures.remove(last);
			if (exception != null) {
				log.error("Something failed", exception);
			}
		} catch (CancellationException | ExecutionException e) {
			log.error("Counting subjects or objects failed", e);
		} catch (TimeoutException e) {
			// This is ok we just try again in the loop.
		}
		if (loop == 60) {

			logRunningQueries();
			loop = 0;
		}
		loop++;
		return loop;
	}

	private void logRunningQueries() {
		// We make a defensive copy of the tasks to avoid concurrent modification
		// exceptions
		for (var qc : List.copyOf(tasks))
			if (qc.isRunning())
				log.info("Running: " + qc.getClass() + " -> " + qc.getQuery());
	}

	private CommonVariables scheduleCounters(ServiceDescription sd, Consumer<ServiceDescription> saver, ReadWriteLock rwLock) {

		CommonVariables cv = new CommonVariables(sd, repository, saver, rwLock, limit, finishedQueries);
		determineGraphNames(sd, saver, rwLock);

		counters.countDistinctObjectsSubjectsInDefaultGraph(cv, countDistinctSubjects, countDistinctObjects);

		for (GraphDescription gd : getGraphs()) {
			counters.countSpecifics(cv, gd, countDistinctSubjects, countDistinctObjects);
		}

		// Ensure that we first do the big counts before starting on counting the
		// smaller sets.
		for (GraphDescription gd : getGraphs()) {
			counters.findPredicatesAndClasses(cv, knownPredicates, gd, findDistinctClasses, findPredicates,
					detailedCount, classExclusion);
		}
		return cv;
	}

	/**
	 * Ensure that the graph description exists so that we won't have an issue
	 * accessing them at any point.
	 * 
	 * @param sd
	 * @param saver
	 * @param writeLock
	 */
	private void determineGraphNames(ServiceDescription sd, Consumer<ServiceDescription> saver, ReadWriteLock rwLock) {
		if (graphNames.isEmpty()) {
			CommonVariables cv = new CommonVariables(sd, repository, saver, rwLock, limit, finishedQueries);
			findingGraphs = counters.findAllGraphs(cv);
		} else {
			for (var graphName : getGraphNames()) {
				getOrCreateGraphDescriptionObject(graphName, sd, rwLock);
			}
		}
	}

	private Set<String> getGraphNames() {
		if (findingGraphs != null && !findingGraphs.isDone()) {
			findingGraphs.join();
		}
		return graphNames;
	}

	private Collection<GraphDescription> getGraphs() {
		if (findingGraphs != null && !findingGraphs.isDone()) {
			findingGraphs.join();
		}
		return sd.getGraphs();
	}

	private GraphDescription getOrCreateGraphDescriptionObject(String graphName, ServiceDescription sd, ReadWriteLock rwLock) {
		GraphDescription pgd = sd.getGraph(graphName);
		if (pgd == null) {
			pgd = new GraphDescription();
			pgd.setGraphName(graphName);
			Lock writeLock = rwLock.writeLock();
			try {
				writeLock.lock();
				sd.putGraphDescription(pgd);
			} finally {
				writeLock.unlock();
			}
		}
		return pgd;
	}

	private void updateGraph(RepositoryConnection connection, IRI voidGraphUri)
			throws RepositoryException, RDFParseException, IOException {
		log.debug("Updating {}", voidGraphUri);
		connection.begin();
		connection.add(sdFile, voidGraphUri);
		connection.commit();
		log.debug("Updating {} done", voidGraphUri);
	}

	private void clearGraph(RepositoryConnection connection, IRI voidGraphUri)
			throws RepositoryException, MalformedQueryException {

		try {
			connection.begin();
			final Update prepareUpdate = connection.prepareUpdate(QueryLanguage.SPARQL,
					"DROP SILENT GRAPH <" + voidGraphUri + ">");
			prepareUpdate.execute();
			connection.commit();
		} catch (UpdateExecutionException e1) {
			log.error("Clearing graph failed", e1);
			connection.rollback();
		}
	}

	public Repository getRepository() {
		return repository;
	}

	public void setRepository(Repository repository) {
		this.repository = repository;
	}

	public void setGraphNames(Set<String> graphNames) {
		this.graphNames = graphNames;
	}

	public void setCountDistinctClasses(boolean countDistinctClasses) {
		this.findDistinctClasses = countDistinctClasses;
	}

	public void setCountDistinctSubjects(boolean countDistinctSubjects) {
		this.countDistinctSubjects = countDistinctSubjects;
	}

	public void setCountDistinctObjects(boolean countDistinctObjects) {
		this.countDistinctObjects = countDistinctObjects;
	}

	public void setFindPredicates(boolean findPredicates) {
		this.findPredicates = findPredicates;
	}

	public void setDetailedCount(boolean detailedCount) {
		this.detailedCount = detailedCount;
	}

	public void setOntologyGraphNames(List<String> ontologyGraphNames) {
		this.ontologyGraphNames = ontologyGraphNames;
	}

	public void setSdFile(File sdFile) {
		this.sdFile = sdFile;
	}

	public void setForcedRefresh(boolean forcedRefresh) {
		this.forcedRefresh = forcedRefresh;
	}

	public void setIriOfVoidAsString(String iriOfVoidAsString) {
		this.iriOfVoidAsString = iriOfVoidAsString;
	}

	public void setIriOfVoid(IRI iriOfVoid) {
		this.iriOfVoid = iriOfVoid;
		this.iriOfVoidAsString = iriOfVoid.stringValue();
	}

	public void setKnownPredicates(Set<IRI> knownPredicates) {
		this.knownPredicates = knownPredicates;
	}

	public void setCommaSeperatedGraphs(String commaSeperatedGraphs) {
		this.commaSeperatedGraphs = commaSeperatedGraphs;
	}

	public void setCommaSeperatedKnownPredicates(String commaSeperatedKnownPredicates) {
		this.commaSeperatedKnownPredicates = commaSeperatedKnownPredicates;
	}

	public void setAddResultsToStore(boolean add) {
		this.addResultsToStore = add;
	}

	/**
	 * The IRI of the endpoint
	 * 
	 * @param rl
	 */
	public void setRepositoryLocator(String rl) {
		this.repositoryLocator = rl;
	}

	public void setDataVersion(String dataVersion) {
		this.dataVersion = dataVersion;
	}

	public void setDataReleaseDate(String dataReleaseDate) {
		this.dataReleaseDate = dataReleaseDate;
	}

	public void setOptimizeFor(String optimizeFor) {
		this.optimizeFor = optimizeFor;
	}

	public void setMaxConcurrency(int maxConcurrency) {
		this.maxConcurrency = maxConcurrency;
	}
	
	public void setClassExclusion(String classExclusion) {
		this.classExclusion = classExclusion;
	}

	public void setFindDistinctClasses(boolean findDistinctClasses) {
		this.findDistinctClasses = findDistinctClasses;
	}

	public void setVirtuosoJdcb(String virtuosoJdcb) {
		this.virtuosoJdcb = virtuosoJdcb;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	//For testing purposes
	int getScheduledQueries() {
		return scheduledQueries.get();
	}

	//For testing purposes
	int getFinishedQueries() {
		return finishedQueries.get();
	}
}
