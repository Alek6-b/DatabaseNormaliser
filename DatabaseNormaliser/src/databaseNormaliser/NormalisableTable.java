package databaseNormaliser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NormalisableTable extends Table implements Normalisable {
	private DependencyNormaliser dependencies;

	private List<Table> normalForm = null;

	public NormalisableTable(Collection<String> attributes) {
		super(attributes);
		dependencies = new DependencyNormaliser(attributes);
	}

	public void addDependency(Dependency d) {
		normalForm = null;
		dependencies.add(d);
	}

	public void addDependencies(Collection<Dependency> d) {
		normalForm = null;
		dependencies.addAll(d);
	}

	/**
	 * Returns the normal form, calculating it if necessary.
	 * 
	 * @see #normalise()
	 * 
	 * @return The object's normal form.
	 * @throws MissingDependencyException
	 * @throws InterruptedException
	 */
	public List<Table> getNormalised() {
		if (normalForm == null) {
			try {
				normalise();
			} catch (InterruptedException e) {
				return null;
			}
		}
		return normalForm;
	}

	/**
	 * Invokes the normalisation algorithm without returning the normal form, which
	 * will be stored within the table and can be retrieved with getNormalised()
	 * later. It is recalculated only after a dependency modification.
	 * 
	 * @throws InterruptedException
	 * @throws MissingDependencyException
	 * 
	 */
	private void normalise() throws InterruptedException {
		List<String> nullable = new ArrayList<String>();
		for (List<Serializable> l : this.entries) {
			for (Serializable s : l) {
				if (s.toString().trim().contentEquals("")|| s.toString().trim().toLowerCase().contentEquals("null"))
					nullable.add(attributes.get(l.indexOf(s)));
			}
		}
		dependencies.setBlacklist(nullable);
		normalForm = dependencies.normalise();
		fillTables(normalForm);
	}

	private void fillTables(List<Table> tables) {
		tables.forEach((t) -> {
			this.entries.forEach((r) -> {
				List<Serializable> newEntry = new ArrayList<Serializable>();
				t.getAttributes().forEach((a) -> newEntry.add(r.get(attributes.indexOf(a))));
				t.addEntry(newEntry);
			});
		});

	}

	@Override
	public List<Dependency> getDependencies() {
		return dependencies.getAll();
	}
}
