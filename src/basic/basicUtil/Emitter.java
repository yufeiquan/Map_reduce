package basic.basicUtil;

import java.io.IOException;

import basic.tasks.MapTask;
import basic.tasks.ReduceTask;

/**
 * Provides an interface for {@link MapTask}s and {@link ReduceTask}s to use to
 * output a key/value pair to the framework.
 * 
 * For example, the {@link MapTask#execute(InputStream, Emitter emitter)} method
 * should be passed an {@link Emitter} that emits key/value pairs to an
 * intermediary results file in the map worker's intermediate data directory.
 */
public interface Emitter {

	/**
	 * Outputs a key/value pair.
	 * 
	 * @param key
	 *            The key to output.
	 * @param value
	 *            The value to output.
	 * 
	 * @throws IOException
	 *             If an I/O error occurs.
	 */
	void emit(String key, String value) throws IOException;

}
