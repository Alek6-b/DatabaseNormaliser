package it.unisa.databaseNormaliser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class AttributeKeyNormaliser extends AbstractSingleTableNormaliser {

	private List<FunctionalDependency> dependencies = null;
	private Set<String> key;

	public AttributeKeyNormaliser(Table table, List<FunctionalDependency> dependencies) {
		this.table = table;
		this.dependencies = new ArrayList<>(dependencies);
	}

	public List<Table> normalise() {
		if (!FunctionalDependency.checkValid(dependencies, table.attributes()))
			throw new IllegalArgumentException("Dependencies not valid.");
		key = (table.key() != null) ? table.key() : guessMainKey();
		dependencies.addFirst(new FunctionalDependency(key, table.attributes()));

		Map<String, Set<String>> attributeKeyMap = new ConcurrentHashMap<>();
		table.attributes().forEach(a -> attributeKeyMap.put(a, key));

		ExecutorService exe = Executors.newCachedThreadPool();
		var dependencyMap = FunctionalDependency.closureMap(dependencies);
		dependencyMap.forEach((k,v) -> v.removeAll(k));
		attributeKeyMap.keySet()
				.forEach(attribute -> exe.execute(() -> computeKey(attribute, dependencyMap, attributeKeyMap)));
		exe.shutdown();
		try {
			exe.awaitTermination(2, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
		return buildTables(attributeKeyMap);
	}

	private <T> Collection<T> sum(Collection<T> a, Collection<T> b) {
		return Stream.concat(a.stream(), b.stream()).toList();
	}
	
	/**
	 * Generates a collection of tables from the attributeKeyMap. Once the map is
	 * computed, it contains a non-transitive, non-partial key for each attribute.
	 * Given that, a normal form is created by grouping each attribute with its
	 * assigned key.
	 * 
	 * @param attributeKeyMap
	 * @return
	 */
	private List<Table> buildTables(Map<String, Set<String>> attributeKeyMap) {

		Map<Set<String>, Set<String>> finalMap = new HashMap<>();
		attributeKeyMap.forEach((attribute, key) -> {
			var v = new LinkedHashSet<>(Collections.singleton(attribute));
			v.removeAll(key);
			finalMap.merge(key, v, (a, b) -> {
				a.addAll(b);
				return a;
			});
		});
		
		//Cycle handling
		var toDelete = new ArrayList<Set<String>>();
		for (var i : finalMap.entrySet()) {
			for (var j : finalMap.entrySet()){
				if (!i.getKey().equals(j.getKey()) && sum(i.getKey(),i.getValue()).containsAll(sum(j.getKey(),j.getValue())))
					toDelete.add(j.getKey());
			}
		}
		toDelete.forEach(finalMap::remove);

		List<Table> normalForm = new ArrayList<>();
		finalMap.forEach((k, v) -> {
			List<String> tableAttributes = new ArrayList<>(k);
			tableAttributes.addAll(v);
			normalForm.add(new Table(tableAttributes, k));
		});
		if (normalForm.stream().noneMatch(t -> t.attributes().containsAll(key)))
			normalForm.add(new Table(key, key));
		this.table.fill(normalForm);
		return normalForm;
	}

	/**
	 * Computes a key for an attribute, such that the attribute depends on it
	 * directly and neither transitively nor partially.
	 * 
	 * @param attribute
	 * @param dependencyMap
	 * @param attributeKeyMap
	 */
	private void computeKey(String attribute, Map<Set<String>, Set<String>> dependencyMap,
			Map<String, Set<String>> attributeKeyMap) {
		// List of all determiners which may determine the given attribute.
		List<Set<String>> toCheck = new ArrayList<Set<String>>(dependencyMap.entrySet().stream().filter(e -> e.getValue().contains(attribute)).map(Map.Entry::getKey).toList());
		List<Set<String>> checklist = new ArrayList<>();
		List<Set<String>> checked = new ArrayList<>();
		Set<String> tmpKey = attributeKeyMap.get(attribute);
		checklist.addAll(toCheck);
		checked.add(tmpKey);

		do {
			checklist.removeAll(checked);
			checked.clear();
			for (var k : checklist)
				// Checks if keyOf(attribute) -> attribute might be a
				// partial dependency, and replaces it if so.
				if (tmpKey.containsAll(k)) {
					attributeKeyMap.put(attribute, k);
					tmpKey = k;
					checked.add(k);
				}
			for (var k : checklist)
				// Checks if keyOf(attribute) -> attribute might be a
				// transitive dependency, and replaces it if so.
				if (dependencyMap.get(tmpKey).containsAll(k)) {
					attributeKeyMap.put(attribute, k);
					tmpKey = k;
					checked.add(k);
				}
		} while (!checked.isEmpty());
	}

	/**
	 * Computes a suitable candidate key for the initial table.
	 *
	 * @return
	 */
	private Set<String> guessMainKey() {
		var key = new ConcurrentSkipListSet<>(table.attributes());
		for (String s : key)
			if (FunctionalDependency.closure(dependencies, setDiff(key, List.of(s))).containsAll(table.attributes()))
				key.remove(s);
		return key;
	}

	/**
	 * A utility function that returns a collection that contains the set difference
	 * between the other two collections.
	 * 
	 * @param <T> The type of Object within the Collections.
	 * @return
	 */
	private static <T> Collection<T> setDiff(Collection<T> a, Collection<T> b) {
		Collection<T> out = new ArrayList<>(a);
		out.removeAll(b);
		return out;
	}
}