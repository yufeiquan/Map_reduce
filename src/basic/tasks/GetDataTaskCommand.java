package basic.tasks;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import basic.basicUtil.Partition;
import basic.basicUtil.WorkerInfo;
import basic.util.KeyValuePair;
import basic.util.WorkerStorage;

public class GetDataTaskCommand extends WorkerCommand {

	private static final long serialVersionUID = 363270922453490726L;
	// Maps each partition to the sections
	private final Map<Partition, Set<Integer>> relationship;

	private final WorkerInfo sender;
	// Used for hashing
	private final int workerServerNum;

	public GetDataTaskCommand(WorkerInfo sender, int workerServerNum) {
		relationship = new HashMap<Partition, Set<Integer>>();
		this.sender = sender;
		this.workerServerNum = workerServerNum;
	}

	public Map<Partition, Set<Integer>> getRelationship() {
		return relationship;
	}

	public void addRelationship(Partition p, Set<Integer> s) {
		if (relationship.containsKey(p)) {
			for (Integer i : s) {
				if (!relationship.get(p).contains(i))
					relationship.get(p).add(i);
			}
		}
		// Not found previously, so update the relationship
		else
			relationship.put(p, s);
	}

	@Override
	public void run() {
		Socket socket = getSocket();
		FileInputStream in = null;
		ObjectOutputStream out = null;
		BufferedReader reader = null;
		String interDir = WorkerStorage.getIntermediateResultsDirectory(sender.getName());
		List<KeyValuePair> result = new ArrayList<KeyValuePair>();
		for (Partition p : relationship.keySet()) {
			String name = interDir + "\\" + p.getPartitionName() + ".txt";
			try {
				in = new FileInputStream(name);
				reader = new BufferedReader(new InputStreamReader(in));
				String line = "";
				while (true) {
					line = reader.readLine();
					if (line == null)
						break; // Same as EOF
					String[] v = line.split("\\W+");
					if (v.length != 2)
						throw new IOException();
					if (relationship.get(p).contains(Math.abs(v[0].hashCode() % workerServerNum))) {
						KeyValuePair pair = new KeyValuePair(v[0], v[1], Integer.parseInt(p.getPartitionName()));
						result.add(pair);
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println("Wrong contents or format.");
				e.printStackTrace();
			} finally {
				if (reader != null)
					try {
						reader.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		}
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
			for (KeyValuePair pair : result) {
				out.writeObject(pair);
				out.reset();
			}
			out.writeObject(new Integer(1)); // As an indicator of EOF
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
