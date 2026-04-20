package it.unisa.databaseNormaliser;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/*	Normalise the table, returning set list of tables as set result. Return null if the table is already normalised.
 * 
 */
public record Table(List<String> attributes, Set<String> key, List<List<? extends Serializable>> entries) {	
	public Table(Collection<String> attributes) {
		this(new ArrayList<String>(attributes),null,new ArrayList<List<? extends Serializable>>());
	}

	public Table(Collection<String> attributes, Collection<String> key) {
		this(new ArrayList<String>(attributes),new LinkedHashSet<String>(key), new ArrayList<List<? extends Serializable>>());
	}

	public void addEntry(List<? extends Serializable> entry) {
		entries.add(new ArrayList<Serializable>(entry));
	}

	void fill(List<Table> derivedTables) {
		derivedTables.forEach((t) -> {
			this.entries.forEach((r) -> {
				List<Serializable> newEntry = new ArrayList<Serializable>();
				t.attributes().forEach(
						(a) -> newEntry.add(r.get(attributes.indexOf(a))));
				t.addEntry(newEntry);
			});
		});
	}
}
