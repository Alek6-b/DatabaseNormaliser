package databaseNormaliser;

import java.lang.System.Logger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DependencyNormaliser {
	private List<String> attributes;
	ConcurrentMap<String, Set<String>> attributeKeys;
	ConcurrentMap<SortedSet<String>, SortedSet<String>> dependencies;
	Set<String> mainKey;
	List<String> keyBlacklist;
	int DETERMINER_MAX_SIZE = 2;
	double BS_FACTOR = 0.1;

	public DependencyNormaliser(Collection<String> attributes) {
		dependencies = new ConcurrentHashMap<SortedSet<String>, SortedSet<String>>();
		attributeKeys = new ConcurrentHashMap<String, Set<String>>();
		this.attributes = new ArrayList<String>(attributes);
		keyBlacklist = new ArrayList<String>();
	}

	public void add(Dependency d) {
		if (d.domain().size() <= DETERMINER_MAX_SIZE)
			dependencies.merge(d.domain(), d.codomain(), (a, b) -> {
				a.addAll(b);
				return a;
			});
	}
	
	public void setBlacklist(Collection<String> b) {
		keyBlacklist.clear();
		keyBlacklist.addAll(b);
	}
	
	public Collection<String> getBlacklist(){
		return keyBlacklist;
	}

	public void addAll(Collection<Dependency> d) {
		Dependency.compress(d).forEach(this::add);
	}

	public Set<String> get(Collection<String> key) {
		return codomain(new ConcurrentSkipListSet<String>(key));
	}

	public List<Dependency> getAll() {
		return dependencies.entrySet().stream().map((e) -> new Dependency(e.getKey(), e.getValue())).toList();
	}

	public SortedSet<String> guessKey() {
		return dependencies.keySet().stream().filter((a) -> cover(a).size() == attributes.size()).min(Comparator.comparingInt(SortedSet::size)).orElse(new ConcurrentSkipListSet<String>(attributes));
	}

	private Set<String> keyOf(String s) {
		return attributeKeys.get(s);
	}
	
	public List<Table> normalise() throws InterruptedException {
		dependencies.forEach((k,v) -> {
			for (String s : keyBlacklist)
				if (k.contains(s)) {
					dependencies.remove(k);
					return;
				}
		});
		List<String> modified = new LinkedList<String>();
		this.mainKey = guessKey();
		System.Logger l = System.getLogger(this.getClass().getName());
		l.log(Logger.Level.INFO, "Main Key: "+mainKey);
		attributes.forEach((a) -> attributeKeys.put(a, mainKey));
		do {
			modified.clear();
			for (var d : dependencies.entrySet())
				for (String s : d.getValue())
					if (checkTransitive(keyOf(s), d.getKey()) || checkPartial(keyOf(s), d.getKey())) {
						dependencies.getOrDefault(keyOf(s), new ConcurrentSkipListSet<String>()).remove(s);
						attributeKeys.put(s, d.getKey());
						modified.add(s);
					}
			System.getLogger(this.getClass().getName()).log(Logger.Level.INFO, "Modified:" + modified);
		} while (modified.size() > 0);

		var toAdd = new HashSet<String>(attributes);
		Map<Set<String>, Set<String>> finalMap = new HashMap<Set<String>, Set<String>>();
		attributeKeys.forEach((att, key) -> finalMap.merge(key, new HashSet<String>(List.of(att)), (a, b) -> {
			a.addAll(b);
			return a;
		}));
		List<Table> normalForm = new LinkedList<Table>();

		finalMap.entrySet().stream().sorted(Comparator.comparingInt((e)-> -e.getValue().size())).forEach((e) -> {
			if (e.getKey().size() < attributes.size()) {
				List<String> tmp = new ArrayList<String>(e.getValue());
				tmp.retainAll(toAdd);
				toAdd.removeAll(tmp);
				if (!tmp.isEmpty())
					normalForm.add(new Table(setSum(e.getKey(), tmp), e.getKey()));
			}
		});
		return normalForm;

	}

	private Table buildTable(Collection<String> key, Collection<String> toDo) {
		var secondary = new ArrayList<String>(get(key));
		secondary.retainAll(toDo);
		if (secondary.isEmpty()) {
			return null;
		}
		toDo.removeAll(secondary);
		secondary.sort(Comparator.comparingInt(attributes::indexOf));
		return new Table(setSum(key, secondary), key);
	}

	/**
	 * Removes empty dependencies. Eg. X -> {}
	 */
	private int clean() {
		int removed = 0;
		synchronized (this) {
			for (Set<String> k : dependencies.keySet())
				if (dependencies.remove(k, Collections.emptySet()))
					removed++;
		}

		return removed;
	}

	/**
	 * Gets the mapped codomain of the given domain. If there isn't one, returns an
	 * empty set instead.
	 * 
	 * @param k
	 * @return
	 */
	private Set<String> codomain(Set<String> k) {
		return dependencies.get(k);
	}

	/**
	 * Computes the cover of a key given a subset of the dependencies in the map.
	 * 
	 * @param key
	 * @param value
	 * @param keySet
	 * @return
	 */
	private SortedSet<String> cover(Collection<String> key, Collection<SortedSet<String>> keySet) {
		SortedSet<String> out = new ConcurrentSkipListSet<String>();
		SortedSet<String> tmp = new ConcurrentSkipListSet<String>(setSum(key, get(key)));
		do {
			out.addAll(tmp);
			keySet.forEach((k) -> {
				if (out.containsAll(k))
					tmp.addAll(codomain(k));
			});
		} while (!(out.containsAll(attributes) | out.equals(tmp)));
		return out;
	}

	private SortedSet<String> cover(Set<String> e) {
		return cover(e, dependencies.keySet());
	}

	/**
	 * Returns the set difference A \ B, such that the returned set contains all
	 * elements in A except any elements in B.
	 * 
	 * @param a: The minuend set.
	 * @param b: The subtrahend set.
	 * @return
	 */
	private <T> Collection<T> difference(Collection<T> a, Collection<T> b) {
		Collection<T> out = new HashSet<T>(a);
		out.removeAll(b);
		return out;
	}

	private Set<String> keyMerger(Set<String> a, Set<String> b) {
		return (checkTransitive(a, b) || checkPartial(a, b)) ? b : a;
	}

	private boolean checkTransitive(Set<String> a, Set<String> b) {
		return (a.equals(new HashSet<String>(attributes)) || dependencies.containsKey(a) && dependencies.get(a).containsAll(b));
	}

	private boolean checkPartial(Set<String> a, Set<String> b) {
		if (a.equals(b))
			return false;
		if (a.containsAll(b)) {
			if (setSum(b, dependencies.get(b)).containsAll(a) && dependencies.containsKey(a))
				dependencies.remove(a);
			return true;
		}
		return false;
	}

	private boolean pairNormalise(SortedSet<String> k1, SortedSet<String> k2) {
		Logger l = System.getLogger(this.getClass().getName());
		boolean a, b, c, d;
		// l.log(System.Logger.Level.INFO, String.format("Processing dependencies: %s
		// and %s.", k1, k2));
		try {
			a = removePartial(k1, k2);
			b = removePartial(k2, k1);
			c = removeTransitive(k1, k2);
			d = removeTransitive(k2, k1);
			l.log(System.Logger.Level.INFO, String.format("Removed %d redundant dependencies.", clean()));
			return (a || b || c || d);
		} catch (Exception e) {
			l.log(System.Logger.Level.ERROR, e);
			return false;
		}

	}

	/**
	 * Checks and removes the partial dependencies between a pair of dependencies,
	 * checking the first against the second. <br>
	 * Eg. If XY -> AZ and X -> Z, removes Z from the first dependency.
	 * 
	 * Also checks if the second mapping implies redundant attributes in the first,
	 * and eliminates the redundancy if so.<br>
	 * Eg. If XY -> AZ and X -> Y, adds AZ to the second dependency and empties the
	 * first.
	 * 
	 * @param k1 : The key that would contain the other.
	 * @param k2 : The key that would be a subset of the other.
	 * 
	 * @return true if the function has modified the map, false otherwise.
	 */
	private boolean removePartial(SortedSet<String> k1, SortedSet<String> k2) {
		if (k1.equals(k2) || codomain(k1).isEmpty() || codomain(k2).isEmpty()) {
			return false;
		}
		if (k1.containsAll(k2)) {
			boolean out = codomain(k1).removeAll(codomain(k2));
			if (setSum(k2, codomain(k2)).containsAll(k1)) {
				out = (codomain(k2).addAll(difference(codomain(k1), k2)) || out);
				dependencies.remove(k1);
			}
			return out;
		}
		return false;
	}

	/**
	 * Checks and removes the transitive dependencies between a pair of
	 * dependencies. Also checks the cover of the second mapping, to remove more
	 * deeper transitive dependencies. <br>
	 * Eg. If X->YZ and Y -> Z, removes Z from the first dependency.
	 * 
	 * 
	 * @param k1 : The key on which the other key might depend.
	 * @param k2 : The key which the first key might determine.
	 * 
	 * @return true if the map has been modified, false otherwise
	 */
	private boolean removeTransitive(SortedSet<String> k1, SortedSet<String> k2) {
		if (k1.equals(k2) || codomain(k1).isEmpty() || codomain(k1).isEmpty()) {
			return false;
		}
		if (codomain(k1).containsAll(k2)) {
			return codomain(k1).removeAll(difference(xCover(k2, k1), k2));
		}
		return false;
	}

	private <T> Collection<T> setSum(Collection<T> a, Collection<T> b) {
		Collection<T> out = new LinkedHashSet<T>(a);
		out.addAll(b);
		return out;
	}

	/**
	 * Computes the cover of k1 without considering k2's dependencies. Used for
	 * guaranteeing non-destructive dependency removal.
	 * 
	 * @param k1 The key to get the cover of.
	 * @param k2 The key whose dependencies not to consider.
	 * @return
	 */
	private SortedSet<String> xCover(SortedSet<String> k1, SortedSet<String> k2) {
		return cover(k1, difference(dependencies.keySet(), Collections.singleton(k2)));
	}
}