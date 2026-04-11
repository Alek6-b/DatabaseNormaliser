package databaseNormaliser;

import java.util.List;

public abstract class AbstractSingleTableNormaliser implements Normaliser {
	protected Table table;
	public abstract List<Table> normalise();
}
