package swiss.sib.swissprot.voidcounter.virtuoso;

import static swiss.sib.swissprot.servicedescription.OptimizeFor.VIRTUOSO;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.ClassPartition;
import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.OptimizeFor;
import swiss.sib.swissprot.servicedescription.PredicatePartition;
import swiss.sib.swissprot.voidcounter.CommonGraphVariables;
import swiss.sib.swissprot.voidcounter.CommonVariables;
import swiss.sib.swissprot.voidcounter.FindGraphs;
import swiss.sib.swissprot.voidcounter.QueryCallable;
import swiss.sib.swissprot.voidcounter.Variables;
import swiss.sib.swissprot.voidcounter.sparql.FindNamedIndividualObjectSubjectForPredicateInGraph;
import swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToDistinctClassInOtherGraph;
import swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToDistinctClassInOtherGraphs;
import swiss.sib.swissprot.voidcounter.sparql.IsSourceClassLinkedToTargetClass;
import swiss.sib.swissprot.voidcounter.sparql.SparqlCounters;
import swiss.sib.swissprot.voidcounter.sparql.TripleCount;

public class VirtuosoCounters extends SparqlCounters {
	private static final Logger log = LoggerFactory.getLogger(VirtuosoCounters.class);
	private final ConcurrentMap<String, Roaring64NavigableMap> distinctSubjectIris;
	private final ConcurrentMap<String, Roaring64NavigableMap> distinctObjectIris;

	public VirtuosoCounters(ConcurrentMap<String, Roaring64NavigableMap> distinctSubjectIris,
			ConcurrentMap<String, Roaring64NavigableMap> distinctObjectIris, Function<QueryCallable<?, ? extends Variables>, CompletableFuture<Exception>> schedule) {
		super(OptimizeFor.VIRTUOSO, schedule, schedule);
		this.distinctSubjectIris = distinctSubjectIris;
		this.distinctObjectIris = distinctObjectIris;
	}

	@Override
	public void countDistinctBnodeSubjectsInAgraph(CommonGraphVariables cv) {
		schedule(new CountDistinctBnodeSubjectsInAGraph(cv));
	}

	@Override
	public CompletableFuture<Exception> findAllGraphs(CommonVariables cv) {
		return schedule(new FindGraphs(cv, VIRTUOSO));
	}

	@Override
	public void countDistinctIriSubjectsAndObjectsInAGraph(CommonGraphVariables cv) {
		schedule(new CountDistinctIriSubjectsAndObjectsInAGraphVirtuoso(cv, distinctSubjectIris, distinctObjectIris));
	}

	@Override
	public void countDistinctBnodeObjectsInDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctBnodeObjectsInAllGraphs(cv));
	}

	@Override
	public void countDistinctIriObjectsForDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctIriObjectsInDefaultGraph(cv));
	}

	@Override
	public void countDistinctLiteralObjectsForDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctLiteralObjectsInDefaultGraph(cv));
	}

	@Override
	public void countDistinctIriSubjectsForDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctIriSubjectsInDefaultGraph(cv));
	}

	@Override
	public void countDistinctIriSubjectsInAGraph(CommonGraphVariables cvgd) {
		schedule(new CountDistinctIriSubjectsInAGraphVirtuoso(cvgd, distinctSubjectIris));
	}

	@Override
	public void countDistinctBnodeSubjectsInDefaultGraph(CommonVariables cv) {
		schedule(new CountDistinctBnodeSubjectsInDefaultGraph(cv));
	}

	@Override
	public void countDistinctLiteralObjectsInAGraph(CommonGraphVariables cv) {
		schedule(new CountDistinctLiteralObjects(cv));
	}

	@Override
	public void countTriplesInNamedGraph(CommonGraphVariables cv) {
		schedule(new TripleCount(cv, VIRTUOSO));
	}

	@Override
	public void isSourceClassLinkedToTargetClass(CommonGraphVariables cv, ClassPartition target,
			PredicatePartition predicatePartition, ClassPartition source) {
		schedule(new IsSourceClassLinkedToTargetClass(cv, target, predicatePartition, source, VIRTUOSO));
	}

	@Override
	public void isSourceClassLinkedToDistinctClassInGraphs(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, 
			String classExclusion){
		schedule(new IsSourceClassLinkedToDistinctClassInOtherGraphs(cv, predicatePartition, source, 
				classExclusion, VIRTUOSO));
	}
	
	@Override
	public void isSourceClassLinkedToDistinctClassInOtherGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source, GraphDescription og,
			String classExclusion) {
		schedule(new IsSourceClassLinkedToDistinctClassInOtherGraph(cv, predicatePartition, source, og, 
				classExclusion, this, VIRTUOSO));
	}

	@Override
	public void findDataTypePartition(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		schedule(new FindDataTypeIfNoClassOrDtKnown(cv, predicatePartition, source));
	}

	@Override
	public void findNamedIndividualObjectSubjectForPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition, ClassPartition source) {
		schedule(new FindNamedIndividualObjectSubjectForPredicateInGraph(cv, predicatePartition, source, VIRTUOSO));
	}

	@Override
	public void countUniqueSubjectPerPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition) {
		schedule(new CountUniqueSubjectPerPredicateInGraph(cv, predicatePartition));
	}

	@Override
	public void countUniqueObjectsPerPredicateInGraph(CommonGraphVariables cv,
			PredicatePartition predicatePartition) {
		schedule(new CountUniqueObjectsPerPredicateInGraph(cv, predicatePartition));
	}
	
	@Override
	public boolean allInUnionGraph() {
		return VIRTUOSO.allInUnionGraph();
	}

	@Override
	public void countDistinctIriSubjectsAndObjectsInDefaultGraph(CommonVariables cv) {
		throw new IllegalStateException("Virtuoso has a default graph, should not use this");
	}

	@Override
	public void countDistinctIriObjectsInAGraph(CommonGraphVariables cvgd) {
		schedule(new CountDistinctIriObjectsInAGraphVirtuoso(cvgd, distinctObjectIris));
	}

	@Override
	public void countDistinctBnodeObjectsInAGraph(CommonGraphVariables gdcv) {
		schedule(new CountDistinctBnodeObjectsInAGraphVirtuoso(gdcv, distinctObjectIris));
	}

	public static void writeGraphsWithSerializedBitMaps(File targetFile,
			ConcurrentMap<String, Roaring64NavigableMap> map) {
		if (!targetFile.exists()) {
			try {
				if (!targetFile.createNewFile()) {
					throw new RuntimeException("Can't create file we need later");
				}
			} catch (IOException e) {
				throw new RuntimeException("Can't create file we need later:", e);
			}
		}
		try (FileOutputStream fos = new FileOutputStream(targetFile);
				BufferedOutputStream bos = new BufferedOutputStream(fos);
				ObjectOutputStream dos = new ObjectOutputStream(bos)) {
			dos.writeInt(map.size());
			for (Map.Entry<String, Roaring64NavigableMap> en : map.entrySet()) {
				dos.writeUTF(en.getKey());
				Roaring64NavigableMap value = en.getValue();
				value.runOptimize();
				value.writeExternal(dos);
			}
	
		} catch (IOException e) {
			log.error("IO issue", e);
		}
	}
	
	public void writeGraphs(File distinctSubjectIrisFile,
			File distinctObjectIrisFile, ReadWriteLock rwLock) {
		final Lock readLock = rwLock.readLock();
		try {
			readLock.lock();
			VirtuosoCounters.writeGraphsWithSerializedBitMaps(distinctSubjectIrisFile, distinctSubjectIris);
			VirtuosoCounters.writeGraphsWithSerializedBitMaps(distinctObjectIrisFile, distinctObjectIris);

		} finally {
			readLock.unlock();
		}
	}

	public static ConcurrentMap<String, Roaring64NavigableMap> readGraphsWithSerializedBitMaps(File file, boolean forcedRefresh) {
		ConcurrentHashMap<String, Roaring64NavigableMap> map = new ConcurrentHashMap<>();
		if (file.exists() && !forcedRefresh) {
			try (FileInputStream fis = new FileInputStream(file); ObjectInputStream bis = new ObjectInputStream(fis)) {
				int numberOfGraphs = bis.readInt();
				for (int i = 0; i < numberOfGraphs; i++) {
					String graph = bis.readUTF();
					Roaring64NavigableMap rb = new Roaring64NavigableMap();
					rb.readExternal(bis);
					map.put(graph, rb);
				}
			} catch (IOException e) {
				log.error("IO error", e);
			} catch (ClassNotFoundException e) {
				log.error("Class can't be found code out of sync", e);
			}
		}
		return map;
	}
}
