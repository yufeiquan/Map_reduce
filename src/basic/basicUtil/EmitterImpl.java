package basic.basicUtil;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/** Write this key value pairs to the specified file name */
public class EmitterImpl implements Emitter, Closeable {
	private final PrintWriter mWriter;

	public EmitterImpl(File file) throws FileNotFoundException {
		this.mWriter = new PrintWriter(new FileOutputStream(file), true);
	}

	@Override
	public void emit(String key, String value) throws IOException {
		mWriter.println(key + " " + value);
	}

	@Override
	public void close() throws IOException {
		mWriter.close();
	}

}
