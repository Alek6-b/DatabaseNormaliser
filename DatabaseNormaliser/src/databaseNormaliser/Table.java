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
public class Table{

	protected List<String> key, attributes;
	protected Set<List<Serializable>> entries;
	
	public Table(Collection<String> attributes) {
		super();
		this.key = null;
		this.attributes = new ArrayList<String>(attributes);
		entries = new LinkedHashSet<List<Serializable>>();
	}
	
	public Table(Collection<String> attributes, Collection<String> key) {
		this(attributes);
		this.key = new ArrayList<String>(key);
	}

	public List<String> getAttributes(){
		return attributes;
	}
	public List<String> getKey() {
		return key;
	}
	
	public void addEntry(List<? extends Serializable> entry) {
		entries.add(new ArrayList<Serializable>(entry));
	}
	
	public Set<List<Serializable>> getEntries(){
		return new LinkedHashSet<List<Serializable>>(entries);
	}
}
