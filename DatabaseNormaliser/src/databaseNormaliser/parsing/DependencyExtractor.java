package databaseNormaliser.parsing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;


/**	Utility class which extracts dependency strings from an input stream associated with a file. 
 * 
 */
public class DependencyExtractor {
	private static final Pattern METANOME_ID_PATTERN = Pattern.compile("\\d+\\.(\\w*)\\s+(\\d+)");
	private static final Pattern NUMBERS = Pattern.compile("\\d+");
	private static final Pattern RFD_FILTER = Pattern.compile("TEST\\.|@\\d\\.\\d|,(?=\\s*->)|\\s*DIPENDENZA CHIAVE.*");
	
	public static List<String> extract(InputStream stream) throws IOException{
		{
			BufferedReader b = new BufferedReader(new InputStreamReader(stream));
			b.mark(500);
			String line = b.readLine();
			if (line.contains("DISCOVERED RFDs")) {
				b.reset();
				return extractFromRFD(b);
			}
			if (line.contains("# TABLES")) {
				b.reset();
				return extractFromMetanome(b);
			}
			throw new IllegalArgumentException("FunctionalDependency File Not Recognized.");
		}
	}
	
	public static List<String> extractFromMetanome(BufferedReader b) throws IOException {
		List<String> out = new LinkedList<String>();
		Map<String, String> idMap = new TreeMap<String,String>();
		
		String line = b.readLine();
		while (!line.contains("COLUMN")) line = b.readLine();
		
		line = b.readLine();
		while (!line.contains("RESULTS")) {
			var res = METANOME_ID_PATTERN.matcher(line);
			res.matches();
			idMap.put(res.group(2), res.group(1));
			line = b.readLine();
		}
		line = b.readLine();
		
		while (line != null) {
			out.add(NUMBERS.matcher(line).replaceAll((n) -> {
				return idMap.get(n.group());
			}));
			line = b.readLine();
		}
		return out;
	}
	
	public static List<String> extractFromRFD(BufferedReader b) throws IOException {
		return b.lines().skip(1).map((line) -> RFD_FILTER.matcher(line).replaceAll("")).toList();
	}
}
