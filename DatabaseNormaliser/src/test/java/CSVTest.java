
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.commons.csv.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.CsvSource;

import it.unisa.databaseNormaliser.*;
import it.unisa.databaseNormaliser.parsing.*;


class CSVTest {
	final String TEST_RESOURCE_PATH = "src/test/resources/";

	@ParameterizedTest
	@CsvSource({"disoccupazione","coke","coke2","creditclient","restaurant","titanic"})
	void test(String testCase)
			throws FileNotFoundException, IOException {
		System.out.println("Testing " + testCase + "...");
		File csvFile, depFile;
		csvFile = new File(TEST_RESOURCE_PATH + testCase +".csv");
		depFile = new File(TEST_RESOURCE_PATH + testCase+"_results.txt");

		CSVParser p = CSVParser.parse(csvFile, null, CSVFormat.DEFAULT.builder()
				.setDelimiter(
						DelimiterExtractor.get(new FileInputStream(csvFile)))
				.setHeader().setSkipHeaderRecord(true).setCommentMarker('#')
				.get());
		var db = new Table(p.getHeaderNames());
		for (CSVRecord i : p.getRecords()) {
			db.addEntry(i.toList());
		}
		List<FunctionalDependency> deps = FunctionalDependency.parse(
				DependencyExtractor.extract(new FileInputStream(depFile)));

		new AttributeKeyNormaliser(db, deps).normalise()
				.forEach((t) -> System.out.println("%s with key: %s".formatted(
						t.attributes().toString(), t.key().toString())));
	}

}
