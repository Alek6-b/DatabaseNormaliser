package databaseNormaliser;

import java.util.Collection;
import java.util.List;

public class NormaliserFactory {
	public List<Table> normalise(Collection<Dependency> dependencies,
			Collection<String> attributes) throws InterruptedException {
		return AttributeCenteredNormaliser.normalise(dependencies, attributes);
	}
}
