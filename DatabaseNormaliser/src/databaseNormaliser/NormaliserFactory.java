package databaseNormaliser;

import java.util.Collection;
import java.util.List;

public class NormaliserFactory {
	public List<Table> normalise(Collection<FunctionalDependency> dependencies,
			Collection<String> attributes) throws InterruptedException {
		return AttributeKeyNormaliser.normalise(dependencies, attributes);
	}
}
