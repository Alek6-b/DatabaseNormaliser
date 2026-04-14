package databaseNormaliser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public record FunctionalDependency(Set<String> determiner, Set<String> dependents) {
	public FunctionalDependency(Set<String> determiner, Set<String> dependents) {
		this.determiner = Collections
				.unmodifiableSet(new LinkedHashSet<>(determiner));
		var tmp = new LinkedHashSet<>(dependents);
		tmp.removeAll(determiner);
		this.dependents = tmp;
	}

	public FunctionalDependency(Collection<String> key, Collection<String> value) {
		this(new LinkedHashSet<>(key), new LinkedHashSet<>(value));
	}

	public String toString() {
		return String.join(", ", determiner) + " -> " +String.join(", ", dependents);
	}
	
	private static final Pattern DEPENDENCY_PATTERN = Pattern.compile(".+->.+");

	/**
	 * Generates a FunctionalDependency object from an appropriately formatted string.
	 *
	 * @param dependency
	 *            A string matching the pattern '.+->.+', where the determiner
	 *            and the dependents both take the form of comma-separated
	 *            lists.
	 * @throws IllegalArgumentException
	 *             If the input string doesn't parse as a dependency.
	 */
	public static FunctionalDependency parse(String dependency)
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
		return new FunctionalDependency(key, value);
	}

	/**
	 * Parses multiple dependency strings at once. The returning list is
	 * automatically compressed.
	 *
	 * @param dependencies
	 * @return A list of dependency objects.
	 */
	public static List<FunctionalDependency> parse(Collection<String> dependencies) {
		return compress(dependencies.stream().map(FunctionalDependency::parse).toList());
	}

	public static List<FunctionalDependency> parse(String... dependencies) {
		return parse(List.of(dependencies));
	}
	
	/**
	 * Parses a dependency map, turning it into a collection of FunctionalDependency objects.
	 * 
	 * @param dependencyMap
	 * @return
	 */
	public static List<FunctionalDependency> parse(Map<Set<String>, Set<String>> dependencyMap) {
		List<FunctionalDependency> out = new ArrayList<>(
				dependencyMap.size());
		dependencyMap.forEach((k, v) -> out.add(new FunctionalDependency(k, v)));
		return out;
	}

	/**
	 * Compresses a collection of Dependencies into a smaller collection with
	 * unique determiners, without losing information. Always good to invoke
	 * before working on a large amount of Dependencies.
	 *
	 * @param dependencies
	 * @return A collection of dependencies where each determiner is unique.
	 */
	public static List<FunctionalDependency> compress(
			Collection<FunctionalDependency> dependencies) {
		return mapOf(dependencies).entrySet().stream()
				.map(e -> new FunctionalDependency(e.getKey(), e.getValue())).toList();
	}
	

	public static List<FunctionalDependency> compress(FunctionalDependency... dependencies) {
		return compress(List.of(dependencies));
	}

	/**
	 * Returns a map describing a collection of dependencies.
	 *
	 * @param dependencies
	 * @return
	 */
	public static Map<Set<String>, Set<String>> mapOf(
			Collection<FunctionalDependency> dependencies) {
		return dependencies.stream()
				.collect(Collectors.<FunctionalDependency, Set<String>, Set<String>>toMap(
						FunctionalDependency::determiner, FunctionalDependency::dependents,
						(a, b) -> {
							a.addAll(b);
							return a;
						}));
	}

}
