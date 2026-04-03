package databaseNormaliser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DependencyNormaliser {
	private List<String> attributes;
	ConcurrentMap<String, SortedSet<String>> attributeKeys;
	ConcurrentMap<SortedSet<String>, SortedSet<String>> dependencies;
	List<String> keyBlacklist;
	SortedSet<String> mainKey;

	public DependencyNormaliser(Collection<String> attributes) {
		dependencies = new ConcurrentHashMap<>();
		attributeKeys = new ConcurrentHashMap<>();
		this.attributes = new ArrayList<>(attributes);
		keyBlacklist = new ArrayList<>();
		mainKey = null;
	}

	public DependencyNormaliser(Collection<String> attributes,
			Collection<String> key) {
		this(attributes);
		mainKey = Collections
				.unmodifiableSortedSet(new ConcurrentSkipListSet<>(key));
	}

	public void add(Dependency d) {
		dependencies.merge(d.determiner(), d.dependents(), (a, b) -> {
			a.addAll(b);
			return a;
		});
	}

	public void setBlacklist(Collection<String> b) {
		keyBlacklist.clear();
		keyBlacklist.addAll(b);
	}

	public Collection<String> getBlacklist() {
		return keyBlacklist;
	}

	public void addAll(Collection<Dependency> d) {
		Dependency.compress(d).forEach(this::add);
	}

	public Set<String> dependents(Collection<String> key) {
		return dependents(new ConcurrentSkipListSet<>(key));
	}

	public List<Dependency> getAll() {
		return dependencies.entrySet().stream()
				.map(e -> new Dependency(e.getKey(), e.getValue())).toList();
	}

	/**
	 * Returns the currently assigned determiner of the given attribute, which
	 * may eventually be used as a table key. If it's not assigned, returns the
	 * mainKey.
	 * 
	 * @param attribute
	 * @return
	 */
	private SortedSet<String> keyOf(String attribute) {
		return attributeKeys.getOrDefault(attribute, mainKey);
	}

	private boolean isKey(SortedSet<String> k, String attribute) {
		return dependencies.getOrDefault(k, Collections.emptySortedSet())
				.contains(attribute);
	}

	private SortedSet<String> dep(Set<String> k) {
		return dependencies.getOrDefault(k, new ConcurrentSkipListSet<>());
	}

	private boolean checkNormalise(SortedSet<String> k1, SortedSet<String> k2) {
		return k1.containsAll(k2) || dep(k1).containsAll(k2);
	}

	private void normaliseStep(
			Map<SortedSet<String>, SortedSet<String>> checkMap,
			String attribute) {
		var checklist = new ArrayList<>(dependencies.keySet());
		var discard = new ArrayList<Set<String>>();
		do {
			discard.clear();
			for (var k : checklist) {
				// Checks if keyOf(attribute) -> attribute might be a partial or
				// transitive dependency, and replaces it if so.
				if (isKey(k, attribute) && !k.equals(keyOf(attribute))
						&& checkNormalise(keyOf(attribute), k)) {
					discard.add(attributeKeys.put(attribute, k));
				}
			}
			checklist.removeAll(discard);
		} while (!discard.isEmpty());

	}

	private SortedSet<String> guessKey() {
		return dependencies.keySet().stream()
				.sorted(Comparator.comparingInt(SortedSet::size))
				.filter(k -> cover(k).containsAll(attributes)).findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
						"No Candidate Key Detected"));
	}

	public List<Table> normalise() throws InterruptedException {
		if (keyBlacklist.size() > 0) {
			Set<SortedSet<String>> blacklisted = new HashSet<>();
			for (var k : dependencies.keySet())
				for (String s : keyBlacklist)
					if (k.contains(s)) {
						blacklisted.add(k);
					}
			blacklisted.forEach(dependencies::remove);
		}
		mainKey = (mainKey != null) ? mainKey : guessKey();

		ExecutorService exe = Executors.newCachedThreadPool();
		attributes.forEach(a -> exe.execute(() -> {
			attributeKeys.put(a,mainKey);
			Map<SortedSet<String>, SortedSet<String>> checkMap = new ConcurrentHashMap<>(
					dependencies);
			normaliseStep(checkMap, a);
		}));
		exe.shutdown();
		exe.awaitTermination(5, TimeUnit.MINUTES);

		// Constructs a new map which maps every found key with the attributes
		// for which it has been chosen. This will serve as the basis for our normal form.
		Map<Set<String>, Set<String>> finalMap = new HashMap<>();
		attributeKeys.forEach((att, key) -> finalMap.merge(key,
				new HashSet<>(List.of(att)), (a, b) -> {
					a.addAll(b);
					return a;
				}));
		finalMap.forEach((k,v) -> v.removeAll(k));

		List<Table> normalForm = new LinkedList<>();
		var toAdd = new ConcurrentSkipListSet<>(attributes);
		finalMap.entrySet().stream()
				.sorted(Comparator.comparingInt(e -> -e.getValue().size()))
				.forEach(e -> {
					List<String> att = new ArrayList<>(e.getKey());
					List<String> dep = new ArrayList<>(e.getValue());
					dep.retainAll(toAdd);
					toAdd.removeAll(dep);
					att.addAll(dep);
					if (!normalForm.stream()
							.anyMatch(t -> t.getAttributes().containsAll(att)))
						normalForm.add(new Table(att, e.getKey()));
				});
		return normalForm;

	}

	/**
	 * Gets the mapped dependents of the given determiner. If there isn't one,
	 * returns an empty set instead.
	 *
	 * @param k
	 * @return
	 */
	private Set<String> dependents(Set<String> k) {
		return dependencies.getOrDefault(k, new ConcurrentSkipListSet<>());
	}

	/**
	 * Computes the cover of a key given a subset of the dependencies in the
	 * map.
	 *
	 * @param key
	 * @param value
	 * @param keySet
	 * @return
	 */
	private SortedSet<String> cover(Collection<String> key,
			Collection<SortedSet<String>> keySet) {
		SortedSet<String> out = new ConcurrentSkipListSet<>();
		SortedSet<String> tmp = new ConcurrentSkipListSet<>(
				setSum(key, dependents(key)));
		do {
			out.addAll(tmp);
			keySet.forEach(k -> {
				if (out.containsAll(k))
					tmp.addAll(dependents(k));
			});
		} while (!(out.containsAll(attributes) | out.equals(tmp)));
		return out;
	}

	private SortedSet<String> cover(Set<String> e) {
		return cover(e, dependencies.keySet());
	}

	private <T> Collection<T> setSum(Collection<T> a, Collection<T> b) {
		Collection<T> out = new LinkedHashSet<>(a);
		out.addAll(b);
		return out;
	}
}