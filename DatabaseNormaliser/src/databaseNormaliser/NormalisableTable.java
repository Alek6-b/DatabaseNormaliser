package databaseNormaliser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NormalisableTable extends Table implements Normalisable {
	private List<Table> normalForm = null;

	public NormalisableTable(Collection<String> attributes) {
		super(attributes);
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
	public List<Table> getNormalised(Collection<FunctionalDependency> d) {
		if (normalForm == null) {
			try {
				normalise(d);
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
	private void normalise(Collection<FunctionalDependency> d) throws InterruptedException {
		normalForm = new NormaliserFactory().normalise(d, attributes);
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
}
