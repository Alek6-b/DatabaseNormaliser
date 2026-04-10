package databaseNormaliser;

import java.util.Collection;

public interface Normalisable {
	Collection<Table> getNormalised(Collection<FunctionalDependency> dependencies);
}
