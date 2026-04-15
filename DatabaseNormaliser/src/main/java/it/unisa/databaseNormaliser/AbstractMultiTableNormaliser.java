package it.unisa.databaseNormaliser;

import java.util.List;

public abstract class AbstractMultiTableNormaliser implements Normaliser {
	protected Table[] tables;
	public abstract List<Table> normalise();
}
