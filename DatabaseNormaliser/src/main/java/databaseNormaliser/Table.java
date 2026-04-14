package databaseNormaliser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/*	Normalise the table, returning set list of tables as set result. Return null if the table is already normalised.
 * 
 */
public class Table {

	protected Set<String> key = null;
	protected List<String> attributes;
	protected List<List<? extends Serializable>> entries;

	public Table(Collection<String> attributes) {
		super();
		this.key = null;
		this.attributes = new ArrayList<String>(attributes);
		entries = new ArrayList<List<? extends Serializable>>();
	}

	public Table(Collection<String> attributes, Collection<String> key) {
		this(attributes);
		this.key = new LinkedHashSet<String>(key);
	}

	public List<String> getAttributes() {
		return attributes;
	}
	public Set<String> getKey() {
		return key;
	}

	public void addEntry(List<? extends Serializable> entry) {
		entries.add(new ArrayList<Serializable>(entry));
	}

	public void setEntries(List<List<? extends Serializable>> entries) {
		this.entries = entries;
	}

	public List<List<? extends Serializable>> getEntries() {
		return entries;
	}

	void fill(List<Table> derivedTables) {
		derivedTables.forEach((t) -> {
			this.entries.forEach((r) -> {
				List<Serializable> newEntry = new ArrayList<Serializable>();
				t.getAttributes().forEach(
						(a) -> newEntry.add(r.get(attributes.indexOf(a))));
				t.addEntry(newEntry);
			});
		});
	}
}
