package basic.plugin.wordprefix;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import basic.basicUtil.Emitter;
import basic.tasks.ReduceTask;

/**
 * The reduce task for a word-prefix map/reduce computation.
 */
public class WordPrefixReduceTask implements ReduceTask {
	private static final long serialVersionUID = 6763871961687287020L;

	@Override
	public void execute(String key, Iterator<String> values, Emitter emitter) throws IOException {
		Map<String, Integer> countMap = new HashMap<String, Integer>();
		String maxCountWord = null;
		int max = 0;

		while (values.hasNext()) {
			int count = 0;
			String word = values.next();
			if (countMap.containsKey(word)) {
				count = countMap.get(word);
				countMap.put(word, count++);
			} else {
				countMap.put(word, 1);
				count = 1;
			}
			if (count > max) {
				max = count;
				maxCountWord = word;
			}
		}
		emitter.emit(key, maxCountWord);
	}

}
