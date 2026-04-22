import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.aggregator.AggregateWith;
import org.junit.jupiter.params.aggregator.ArgumentsAccessor;
import org.junit.jupiter.params.aggregator.ArgumentsAggregationException;
import org.junit.jupiter.params.aggregator.ArgumentsAggregator;
import org.junit.jupiter.params.provider.CsvSource;

import it.unisa.databaseNormaliser.AttributeKeyNormaliser;
import it.unisa.databaseNormaliser.FunctionalDependency;
import it.unisa.databaseNormaliser.Table;

@ParameterizedClass
@CsvSource({
	"ABCDE, A, A->BCD, B->C",
	"ABCDE, AE, A->BCD, B->C",
	})

record LetterSchemaTestWithKey(String attributes, String key,
		@AggregateWith(Aggregator.class) String... dependencies) {
	/**
	 * @throws java.lang.Exception
	 */
	static public class Aggregator implements ArgumentsAggregator {
		public String[] aggregateArguments(ArgumentsAccessor arg0, ParameterContext arg1)
				throws ArgumentsAggregationException {
			List<String> l = new ArrayList<String>();
			for (int i = 2; i < arg0.size(); i++)
				l.add(arg0.getString(i));
			return l.toArray(String[]::new);
		}
	}
	
	@RepeatedTest(1)
		void test(){
		System.out.println(String.format("Testing with attributes: %s", attributes));
		System.out.println(String.format("Key: %s", key));

		var db = new Table(Util.letterAttributeParse(attributes),Util.letterAttributeParse(key));

		List<FunctionalDependency> d = Arrays.stream(dependencies).map((s) -> {
			System.out.print("Adding dependency:" + s + "\n");
			String[] tmp = s.split("->");
			return new FunctionalDependency(Util.letterAttributeParse(tmp[0]),
					Util.letterAttributeParse(tmp[1]));
		}).toList();
		System.out.print("Result:\n");
		try {
			new AttributeKeyNormaliser(db,d).normalise()
					.forEach((t) -> System.out.println("%s with key: %s"
							.formatted(t.attributes().toString(),
									t.key().toString())));
		} catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
		}

	}

}
