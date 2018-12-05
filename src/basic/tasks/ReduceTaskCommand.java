package basic.tasks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.sun.jmx.snmp.tasks.Task;

import basic.basicUtil.Emitter;
import basic.basicUtil.EmitterImpl;
import basic.util.WorkerStorage;

/**
 * A {@link WorkerCommand} that executes a {@link Task} and sends the calculated
 * result back to the client.
 * 
 * Note that the generic type <code>T</code> must extend {@link Serializable}
 * since we will be writing it over the network back to the client using an
 * {@link ObjectOutputStream}.
 */
public class ReduceTaskCommand extends WorkerCommand {
	private static final long serialVersionUID = -5314076333098679665L;

	private final ReduceTask reduceTask;
	private final String workerName;
	private final static int PARTITION_TOTAL = 10;

	public ReduceTaskCommand(ReduceTask task, String name) {
		reduceTask = task;
		workerName = name;
	}

	@Override
	public void run() {
		Map<String, List<String>> keyValues = new HashMap<String, List<String>>();
		Socket socket = getSocket();
		ObjectOutputStream out = null;
		FileInputStream in = null;
		String inFileName = null;
		String outFileName = null;
		String interRes = WorkerStorage.getIntermediateResultsDirectory(workerName);
		String finalRes = WorkerStorage.getFinalResultsDirectory(workerName);

		for (int i = 0; i < PARTITION_TOTAL; i++) {
			inFileName = interRes + "\\shuffle" + (i + 1) + ".txt";
			BufferedReader reader = null;
			try {
				in = new FileInputStream(inFileName);
				reader = new BufferedReader(new InputStreamReader(in));
				while (true) {
					String line = reader.readLine();
					if (line == null) {
						break;
					}
					String[] s = line.split("\\W+");
					if (!keyValues.containsKey(s[0])) {
						List<String> list = new ArrayList<String>();
						list.add(s[1]);
						keyValues.put(s[0], list);
					} else {
						keyValues.get(s[0]).add(s[1]);
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println("Reduce : readline error!");
				e.printStackTrace();
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		// Write the final reduce result to the file
		try {
			outFileName = finalRes + "\\result.txt";
			Emitter emitter = new EmitterImpl(new File(outFileName));
			for (String key : keyValues.keySet()) {
				Iterator<String> iter = keyValues.get(key).iterator();
				reduceTask.execute(key, iter, emitter);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Write back the result, the reduced file name for this worker that
		// executes this task
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(outFileName);
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
