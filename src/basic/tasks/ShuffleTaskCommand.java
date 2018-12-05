package basic.tasks;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import basic.SenderServer;
import basic.basicUtil.Partition;
import basic.basicUtil.WorkerInfo;
import basic.util.WorkerStorage;

public class ShuffleTaskCommand extends WorkerCommand {

	private static final long serialVersionUID = 963357821931237885L;
	private final WorkerInfo worker;
	private final int workerServerNumber;
	private final Set<Integer> partialSections;
	private final Map<Partition, WorkerInfo> partitionHolder;
	private final List<WorkerInfo> failedWorkerList;
	final ExecutorService mExecutor;

	private static final int POOL_SIZE = Runtime.getRuntime().availableProcessors();

	public ShuffleTaskCommand(WorkerInfo worker, int workerServerNumber, Set<Integer> partialSections, Map<Partition, WorkerInfo> partitionHolder) {
		this.worker = worker;
		this.workerServerNumber = workerServerNumber;
		this.partialSections = partialSections;
		this.partitionHolder = partitionHolder;
		failedWorkerList = new ArrayList<WorkerInfo>();
		mExecutor = Executors.newFixedThreadPool(POOL_SIZE);
	}

	@Override
	public void run() {
		Socket socket = getSocket();
		ObjectOutputStream out = null;
		String interDirectory = WorkerStorage.getIntermediateResultsDirectory(worker.getName());
		Map<WorkerInfo, GetDataTaskCommand> workerLoad = new HashMap<WorkerInfo, GetDataTaskCommand>();
		FileInputStream in = null;
		BufferedReader reader = null;

		for (Partition p : partitionHolder.keySet()) {
			String name = interDirectory + "\\shuffle" + p.getPartitionName() + ".txt";
			String configFileName = interDirectory + "\\shuffle" + p.getPartitionName() + "configStr.txt";
			try {
				// build the working partialSections.
				in = new FileInputStream(name);
				// read the configStr line of file.
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(configFileName)));
				String configStr = reader.readLine();
				// get the partialSections that has already got
				List<String> names;
				if (configStr == null) {
					names = new ArrayList<String>();
					configStr = "";
				} else
					names = Arrays.asList(configStr.split("\\W+"));

				// filter the already got ones.
				int i = 0;
				Set<Integer> set = new HashSet<Integer>();
				set.addAll(partialSections);
				Iterator<Integer> iter = set.iterator();
				while (iter.hasNext()) {
					i = iter.next();
					for (String s : names) {
						if (Integer.parseInt(s) == i) {
							iter.remove();
							break;
						}
					}
				}
				if (set.size() != 0) {
					if (!workerLoad.containsKey(partitionHolder.get(p))) {
						GetDataTaskCommand task = new GetDataTaskCommand(partitionHolder.get(p), workerServerNumber);
						task.addRelationship(p, set);
						workerLoad.put(partitionHolder.get(p), task);
					} else
						workerLoad.get(partitionHolder.get(p)).addRelationship(p, set);
				}
			} catch (FileNotFoundException e) {
				if (!workerLoad.containsKey(partitionHolder.get(p))) {
					GetDataTaskCommand task = new GetDataTaskCommand(partitionHolder.get(p), workerServerNumber);
					task.addRelationship(p, partialSections);
					workerLoad.put(partitionHolder.get(p), task);
				} else
					workerLoad.get(partitionHolder.get(p)).addRelationship(p, partialSections);
			} catch (IOException e) {
				System.err.println("Readline error.");
				e.printStackTrace();
			} finally {
				try {
					if (in != null)
						in.close();
					if (reader != null)
						reader.close();
				} catch (IOException e) {
					System.err.println("Close error at suffle task command.");
				}
			}
		}
		// Send to each destination map worker
		List<SenderServer> sCallables = new ArrayList<SenderServer>();
		for (WorkerInfo i : workerLoad.keySet())
			sCallables.add(new SenderServer(i, workerLoad.get(i), worker, partialSections));

		List<Future<WorkerInfo>> sendResults = null;
		try {
			sendResults = mExecutor.invokeAll(sCallables);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

		// Sanity check and construct the failed worker list
		for (Future<WorkerInfo> future : sendResults) {
			try {
				WorkerInfo w = future.get();
				if (w != null) {
					failedWorkerList.add(w);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
			if (failedWorkerList.size() != 0)
				// Indicates that the shuffle has done
				out.writeObject(failedWorkerList);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
