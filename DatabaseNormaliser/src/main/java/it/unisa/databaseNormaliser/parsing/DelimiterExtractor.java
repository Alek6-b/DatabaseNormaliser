package it.unisa.databaseNormaliser.parsing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.stream.Stream;

public class DelimiterExtractor {
	/**
	 * Guesses the CSV file's delimiter.
	 * 
	 * @param csvStream : An input stream tied to the CSV file.
	 * @return The delimiter string.
	 * @throws IOException
	 */
	public static String get(InputStream csvStream) throws IOException {
		String[] candidates = { ",", ";", "/t" };
		String tmp;
		try (BufferedReader testReader = new BufferedReader(new InputStreamReader(csvStream))) {
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
