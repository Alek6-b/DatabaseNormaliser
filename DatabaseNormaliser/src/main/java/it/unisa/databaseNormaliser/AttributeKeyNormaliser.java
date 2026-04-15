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

public class AttributeKeyNormaliser extends AbstractSingleTableNormaliser {

	private List<FunctionalDependency> dependencies = null;
	private Set<String> tableKey;
	private Map<Set<String>, Set<String>> dependencyMap;

	public AttributeKeyNormaliser(Table table,
			List<FunctionalDependency> dependencies) {
		this.table = table;
		this.dependencies = dependencies;
		this.dependencyMap = FunctionalDependency.mapOf(dependencies);
	}

	public List<Table> normalise() {
		tableKey = (table.getKey() != null) ? table.getKey() : guessMainKey();

		Map<String, Set<String>> attributeKeyMap = new ConcurrentHashMap<>();
		table.getAttributes().forEach(a -> attributeKeyMap.put(a, tableKey));

		ExecutorService exe = Executors.newCachedThreadPool();
		table.getAttributes().forEach(attribute -> exe
				.execute(() -> computeKey(attribute, attributeKeyMap)));
		exe.shutdown();
		try {
			exe.awaitTermination(2, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
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

		List<Table> normalForm = new ArrayList<>();
		finalMap.forEach((k, v) -> {
			List<String> tableAttributes = new ArrayList<>(k);
			tableAttributes.addAll(v);
			if (normalForm.stream().noneMatch(
					t -> t.getAttributes().containsAll(tableAttributes)))
				normalForm.add(new Table(tableAttributes, k));
		});
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
	private void computeKey(String attribute,
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
	private Set<String> cover(Collection<String> key) {
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
	private Set<String> guessMainKey() {
		var key = new ConcurrentSkipListSet<>(table.getAttributes());
		for (String s : key)
			if (cover(setDiff(key, List.of(s))).containsAll(table.getAttributes()))
				key.remove(s);
		return key;
	}

	/**
	 * A utility function that returns a collection that contains the set
	 * difference between the other two collections.
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