package basic.plugin.wordprefix;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import basic.basicUtil.Emitter;
import basic.tasks.MapTask;

/**
 * The map task for a word-prefix map/reduce computation.
 */
public class WordPrefixMapTask implements MapTask {
	private static final long serialVersionUID = 3046495241158633404L;

	public void execute(InputStream in, Emitter emitter) throws IOException {
		Scanner scanner = new Scanner(in);
		scanner.useDelimiter("\\W+");
		while (scanner.hasNext()) {
			String word = scanner.next().trim().toLowerCase();
			emitter.emit(word, word);
			if (word.length() > 1) {
				for (int i = 1; i <= word.length(); i++) {
					String key = word.substring(0, i);
					emitter.emit(key, word);
				}
			}
		}
		scanner.close();
	}
}
