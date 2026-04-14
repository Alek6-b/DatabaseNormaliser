import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.params.aggregator.*;

public class Util {

	static public List<String> attributeParse(String s) {
		return s.chars().mapToObj((c) -> String.valueOf((char) c)).toList();
		
	}

	static public class VarargsStringAggregator implements ArgumentsAggregator {
		public String[] aggregateArguments(ArgumentsAccessor arg0, ParameterContext arg1)
				throws ArgumentsAggregationException {
			List<String> l = new ArrayList<String>();
			for (int i = 1; i < arg0.size(); i++)
				l.add(arg0.getString(i));
			return l.toArray(String[]::new);
		}
	}
}
