package databaseNormaliser;

import java.util.Collection;
import java.util.List;

public interface Normalisable {
	void addDependency(Dependency d);
	void addDependencies(Collection<Dependency> d);
	List<Dependency> getDependencies();
	List<Table> getNormalised();
}
