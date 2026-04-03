package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.stream.Stream;

import org.apache.commons.csv.*;
import org.junit.jupiter.api.Test;

import databaseNormaliser.Dependency;
import databaseNormaliser.NormalisableTable;
import databaseNormaliser.parsing.DependencyExtractor;


class CSVTest {
	@Test
	void test() throws FileNotFoundException, IOException {
		File csv, dep;
		csv = new File("src/test/resources/student.csv");
		dep = new File("src/test/resources/student_results.txt");
		CSVParser p = CSVParser.parse(new BufferedReader(new InputStreamReader(new FileInputStream(csv))), CSVFormat.DEFAULT.builder().setDelimiter(getDelimiter(csv)).setHeader()
				.setSkipHeaderRecord(true).setCommentMarker('#').get());
		NormalisableTable db = new NormalisableTable(p.getHeaderNames());
		for (CSVRecord i : p.getRecords()) {
			db.addEntry(i.toList());
		}
		db.addDependencies(Dependency.parse(DependencyExtractor.extract(new FileInputStream(dep))));
		db.getNormalised().forEach((t) -> System.out
				.println("%s with key: %s".formatted(t.getAttributes().toString(), t.getKey().toString())));
	}
	private String getDelimiter(File f) throws IOException {
		String[] candidates = { ",", ";", "/t" };
		String tmp;
		try (BufferedReader testReader = new BufferedReader(new InputStreamReader(new FileInputStream(f)))) {
			do
				tmp = testReader.readLine();
			while (tmp.isBlank() || tmp.startsWith("#"));

			final String header = tmp;

			return Stream.of(candidates)
					.max(Comparator.comparingInt((s) -> header.split(s).length))
					.get();
		}
	}
}
