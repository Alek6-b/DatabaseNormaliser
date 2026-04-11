/**
 * 
 */
package test;

import java.util.*;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.aggregator.AggregateWith;
import org.junit.jupiter.params.provider.CsvSource;
import databaseNormaliser.*;

/**
 * A simple test class that takes CSV strings as inputs, where each letter
 * corresponds to an attribute.
 * 
 */
@ParameterizedClass
@CsvSource({"ABCDEFG, BC->D, D->EF, A->B, AB->CD, B->C, C->A",
		"ABCD, D->ACD, BC->D", "ABCDEFGH, A->BC, D->EF, G->AD, DE->F",
		"ABCDE,A->BCDE, D->E, B->C, C->D", "ABCDE,A->BCD, B->C"})

record QuickTest(String attributes,
		@AggregateWith(Util.VarargsStringAggregator.class) String... dependencies) {
	/**
	 * @throws java.lang.Exception
	 */

	@RepeatedTest(1)
	void testWithArgumentsAccessor() {
		System.out.println(
				String.format("Testing with attributes: %s", attributes));

		var db = new Table(Util.attributeParse(attributes));

		List<FunctionalDependency> d = Arrays.stream(dependencies).map((s) -> {
			System.out.print("Adding dependency:" + s + "\n");
			String[] tmp = s.split("->");
			return new FunctionalDependency(Util.attributeParse(tmp[0]),
					Util.attributeParse(tmp[1]));
		}).toList();
		System.out.print("Result:\n");
		try {
			new AttributeKeyNormaliser(db,d).normalise()
					.forEach((t) -> System.out.println("%s with key: %s"
							.formatted(t.getAttributes().toString(),
									t.getKey().toString())));
		} catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
		}

	}

}
