package basic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import basic.basicUtil.Emitter;
import basic.basicUtil.EmitterImpl;
import basic.basicUtil.WorkerInfo;
import basic.tasks.GetDataTaskCommand;
import basic.util.KeyValuePair;
import basic.util.WorkerStorage;

/** This class is used as writing partial shuffle results during the shuffle routine 
 * 
 * @author Dingty / Yufei Quan
 *
 */
public class SenderServer implements Callable<WorkerInfo> {
	private WorkerInfo receiver;
	private GetDataTaskCommand task;
	private final WorkerInfo worker;
	private final Set<Integer> partialSections;

	public SenderServer(WorkerInfo receiver, GetDataTaskCommand task, WorkerInfo worker, Set<Integer> partialSections) {
		this.receiver = receiver;
		this.task = task;
		this.worker = worker;
		this.partialSections = partialSections;
	}

	@Override
	public WorkerInfo call() {
		HashMap<Integer, List<KeyValuePair>> result = new HashMap<Integer, List<KeyValuePair>>();
		Socket socket = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;
		try {
			socket = new Socket(receiver.getHost(), receiver.getPort());
			out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(task);
			in = new ObjectInputStream(socket.getInputStream());
			while (true) {
				Object object = in.readObject();
				if (object != null && object instanceof KeyValuePair) {
					KeyValuePair pair = (KeyValuePair) object;
					int index = pair.getIndex();
					if (!result.containsKey(index)) {
						List<KeyValuePair> list = new ArrayList<KeyValuePair>();
						list.add(pair);
						result.put(index, list);
					} else
						result.get(index).add(pair);
				} else if (object != null && object instanceof Integer) {
					// Receives an EOF indicator as specified in the
					// GetDataTaskCommand
					if (((Integer) object).intValue() == 1)
						break;
				} else
					throw new IOException();
			}
		} catch (IOException e) {
			return receiver;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				if (socket != null)
					socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// Record each partition results by writing a file
		BufferedReader reader = null;
		String interDir = WorkerStorage.getIntermediateResultsDirectory(worker.getName());
		for (Integer i : result.keySet()) {
			String configStr = null;
			String configFileName = interDir + "//shuffle" + i.intValue() + "configStr.txt";
			try {
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(configFileName)));
				configStr = reader.readLine();
				List<String> names = new ArrayList<String>();
				if (configStr != null)
					names.addAll(Arrays.asList(configStr.split("\\s*,\\s*")));

				// Sanity check by detecting redundant works
				for (Integer sectionIndex : partialSections) {
					if (!names.contains(sectionIndex.toString()))
						names.add(sectionIndex.toString());
					else
						System.err.println("Sanity check fails.");
				}
				configStr = new String();
				for (String s : names)
					configStr += s + " ";
			} catch (IOException e) {
				configStr = new String();
				for (Integer sectionIndex : partialSections)
					configStr += sectionIndex.toString() + " ";
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			PrintWriter writer = null;
			try {
				writer = new PrintWriter(new FileOutputStream(configFileName));
				writer.println(configStr);
				writer.flush();
			} catch (IOException e) {
			} finally {
				if (writer != null) {
					writer.close();
				}
			}

			// Output the key-value pair
			FileOutputStream output = null;
			Emitter emitter = null;
			String outFileName = interDir + "//shuffle" + i + ".txt";
			try {
				output = new FileOutputStream(outFileName, true);
				emitter = new EmitterImpl(new File(outFileName));
				List<KeyValuePair> rstList = result.get(i);
				for (KeyValuePair pair : rstList)
					emitter.emit(pair.getKey(), pair.getValue());
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (output != null) {
					try {
						output.close();
					} catch (IOException e) {
					}
				}
			}
		}
		return null;
	}
}
