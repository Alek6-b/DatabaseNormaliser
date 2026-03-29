package databaseNormaliser;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.Pattern;

public record Dependency(SortedSet<String> domain, SortedSet<String> codomain) {

	public Dependency(SortedSet<String> domain, SortedSet<String> codomain) {
		this.domain = domain;
		var tmp = new ConcurrentSkipListSet<String>(codomain);
		tmp.removeAll(domain);
		this.codomain = tmp;
	}
	
	public Dependency(Collection<String> key, Collection<String> value) {
		this(Collections.unmodifiableSortedSet(new ConcurrentSkipListSet<String>(key)), new ConcurrentSkipListSet<String>(value));
	}
	private static final Pattern DEPENDENCY_PATTERN = Pattern.compile(".+->.+");

	public static Dependency parse(String dependency) {
		if (!DEPENDENCY_PATTERN.matcher(dependency).matches())
			throw new IllegalArgumentException(dependency + " is not a valid dependency string.");
		List<String> key, value;
		String[] split = dependency.split("->");
		key = Arrays.asList(split[0].split(","));
		value = Arrays.asList(split[1].split(","));
		key.replaceAll(String::trim);
		value.replaceAll(String::trim);
		return new Dependency(key,value);
	}
	
	public static List<Dependency> parse(Collection<String> dependency){
		return compress(dependency.stream().map(Dependency::parse).toList());
	}
	
	public static List<Dependency> compress(Collection<Dependency> dependencies){
		Map<Set<String>,Set<String>> map = new HashMap<Set<String>,Set<String>>();
		List<Dependency> out = new LinkedList<Dependency>();
		dependencies.forEach((e) -> map.merge(e.domain(), e.codomain(), (a,b) -> {a.addAll(b); return a;}));
		map.forEach((k,v) -> out.add(new Dependency(k,v)));
		return out;
	}
}
