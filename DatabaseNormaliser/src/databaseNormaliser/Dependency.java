package databaseNormaliser;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public record Dependency(SortedSet<String> determiner,
		SortedSet<String> dependents) {

	public Dependency(SortedSet<String> determiner,
			SortedSet<String> dependents) {
		this.determiner = Collections.unmodifiableSortedSet(new ConcurrentSkipListSet<String>(determiner));
		var tmp = new ConcurrentSkipListSet<String>(dependents);
		tmp.removeAll(determiner);
		this.dependents = tmp;
	}

	public Dependency(Collection<String> key, Collection<String> value) {
		this(new ConcurrentSkipListSet<String>(key),
				new ConcurrentSkipListSet<String>(value));
	}

	private static final Pattern DEPENDENCY_PATTERN = Pattern.compile(".+->.+");

	/**
	 * Generates a Dependency object from an appropriately formatted string.
	 * 
	 * @param dependency
	 *            A string matching the pattern '.+->.+', where the determiner
	 *            and the dependents both take the form of comma-separated
	 *            lists.
	 * @throws IllegalArgumentException
	 *             If the input string doesn't parse as a dependency.
	 */
	public static Dependency parse(String dependency)
			throws IllegalArgumentException {
		if (!DEPENDENCY_PATTERN.matcher(dependency).matches())
			throw new IllegalArgumentException(
					dependency + " is not a valid dependency string.");
		List<String> key, value;
		String[] split = dependency.split("->");
		key = Arrays.asList(split[0].split(","));
		value = Arrays.asList(split[1].split(","));
		key.replaceAll(String::trim);
		value.replaceAll(String::trim);
		return new Dependency(key, value);
	}

	/**
	 * Parses multiple dependency strings at once. The returning list is
	 * automatically compressed.
	 * 
	 * @param dependencies
	 * @return A list of dependency objects.
	 */
	public static List<Dependency> parse(Collection<String> dependencies) {
		return compress(dependencies.stream().map(Dependency::parse).toList());
	}

	/**
	 * Compresses a collection of Dependencies into a smaller collection,
	 * without losing information. Always good to invoke before working on a
	 * large amount of Dependencies.
	 *
	 * @param dependencies
	 * @return
	 */
	public static List<Dependency> compress(
			Collection<Dependency> dependencies) {
		return dependencies.stream().collect(Collectors
				.<Dependency, SortedSet<String>, SortedSet<String>>toMap(
						Dependency::determiner, Dependency::dependents,
						(a, b) -> {
							a.addAll(b);
							return a;
						}))
				.entrySet().stream()
				.map(e -> new Dependency(e.getKey(), e.getValue())).toList();
	}
}
