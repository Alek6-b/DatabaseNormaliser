package databaseNormaliser;

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

public class AttributeKeyNormaliser {
	public static List<Table> normalise(Collection<FunctionalDependency> dependencies,
			Collection<String> attributes) throws InterruptedException {
		return normalise(dependencies, attributes, null);
	}

	public static List<Table> normalise(Collection<FunctionalDependency> dependencies,
			Collection<String> attributes, Collection<String> tableKey)
			throws InterruptedException {

		Set<String> mainKey = (tableKey != null)
				? Collections.unmodifiableSet(new LinkedHashSet<>(tableKey))
				: guessMainKey(dependencies, attributes);

		Map<String, Set<String>> attributeKeyMap = new ConcurrentHashMap<>();
		attributes.forEach(a -> attributeKeyMap.put(a, mainKey));

		ExecutorService exe = Executors.newCachedThreadPool();
		var dependencyMap = FunctionalDependency.mapOf(dependencies);
		attributes.forEach(attribute -> exe.execute(
				() -> computeKey(attribute, dependencyMap, attributeKeyMap)));
		exe.shutdown();
		exe.awaitTermination(1, TimeUnit.MINUTES);

		return buildTables(attributeKeyMap);
	}

	/**
	 * Generates a collection of tables from the attributeKeyMap. Once the map
	 * is computed, it contains a non-transitive, non-partial key for each
	 * attribute. Given that, a normal form is created by grouping each
	 * attribute with its assigned key.
	 * 
	 * @param attributeKeyMap
	 * @return
	 */
	private static List<Table> buildTables(
			Map<String, Set<String>> attributeKeyMap) {

		Map<Set<String>, Set<String>> finalMap = new HashMap<>();
		attributeKeyMap.forEach((attribute, key) -> {
			var v = new LinkedHashSet<>(Collections.singleton(attribute));
			v.removeAll(key);
			finalMap.merge(key, v, (a, b) -> {
				a.addAll(b);
				return a;
			});
		});

		List<Table> normalForm = new ArrayList<>();
		finalMap.forEach((k, v) -> {
			List<String> tableAttributes = new ArrayList<>(k);
			tableAttributes.addAll(v);
			if (normalForm.stream().noneMatch(
					t -> t.getAttributes().containsAll(tableAttributes)))
				normalForm.add(new Table(tableAttributes, k));
		});
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
	private static void computeKey(String attribute,
			Map<Set<String>, Set<String>> dependencyMap,
			Map<String, Set<String>> attributeKeyMap) {
		// List of all determiners which may determine the given attribute.
		List<Set<String>> checklist = new ArrayList<>(dependencyMap.entrySet()
				.stream().filter(e -> e.getValue().contains(attribute))
				.map(Map.Entry::getKey).toList());

		List<Set<String>> checked = new ArrayList<>();
		Set<String> tmpKey = attributeKeyMap.get(attribute);
		checked.add(tmpKey);

		do {
			checklist.removeAll(checked);
			checked.clear();
			for (var k : checklist)
				// Checks if keyOf(attribute) -> attribute might be a
				// partial or
				// transitive dependency, and replaces it if so.
				if (tmpKey.containsAll(k) || dependencyMap
						.getOrDefault(tmpKey, new HashSet<>()).containsAll(k)) {
					attributeKeyMap.put(attribute, k);
					tmpKey = k;
					checked.add(k);
				}
		} while (!checked.isEmpty());
	}

	/**
	 * Computes the cover of a key.
	 *
	 * @return
	 */
	private static Set<String> cover(Collection<FunctionalDependency> dependencies,
			Collection<String> key) {
		Set<String> out = new HashSet<>();
		Set<String> tmp = new HashSet<>(key);
		do {
			out.addAll(tmp);
			dependencies.forEach(d -> {
				if (out.containsAll(d.determiner()))
					tmp.addAll(d.dependents());
			});
		} while (!out.containsAll(tmp));
		return out;
	}

	/**
	 * Computes a suitable candidate key for the initial table.
	 *
	 * @return
	 */
	private static Set<String> guessMainKey(Collection<FunctionalDependency> dependencies,
			Collection<String> attributes) {
		var key = new ConcurrentSkipListSet<>(attributes);
		for (String s : key)
			if (cover(dependencies, setDiff(key, List.of(s)))
					.containsAll(attributes))
				key.remove(s);
		return key;
	}

	/**
	 * Returns a collection that contains the set difference between the other
	 * two collections.
	 * 
	 * @param <T>
	 *            The type of Object within the Collections.
	 * @return
	 */
	private static <T> Collection<T> setDiff(Collection<T> a, Collection<T> b) {
		Collection<T> out = new ArrayList<>(a);
		out.removeAll(b);
		return out;
	}
}