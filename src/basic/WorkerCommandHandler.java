package basic;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import basic.basicUtil.Partition;
import basic.basicUtil.WorkerInfo;
import basic.callables.MapCallable;
import basic.callables.ReduceCallable;
import basic.callables.ShuffleCallable;
import basic.tasks.MapTask;
import basic.tasks.MapTaskCommand;
import basic.tasks.ReduceTask;
import basic.tasks.ReduceTaskCommand;
import basic.tasks.ShuffleTaskCommand;
import basic.tasks.WorkerCommand;
import basic.util.Log;

/**
 * Now the master server is a client Handles several worker-clients request.
 */
public class WorkerCommandHandler implements Runnable {
	private final Socket mSocket;
	private final List<WorkerInfo> mWorkers;
	private final ExecutorService mExecutorWorkerServer;
	private static final String TAG = "Master server";

	/* These maps keep track of all relationships between workers and partitions */
	private final Map<WorkerInfo, Boolean> workerStatus;
	private final Map<Partition, List<WorkerInfo>> multipleHolders;
	private final Map<Partition, WorkerInfo> singleHolder;
	private final Map<WorkerInfo, Set<Integer>> reduceWorkerLoad;
	// Keep the final result and send back to the client
	private final List<Result> resultList;

	public WorkerCommandHandler(Socket socket, List<WorkerInfo> workers, ExecutorService workerServerService) {
		mSocket = socket; // mSocket is the real client socket
		mWorkers = workers;
		mExecutorWorkerServer = workerServerService;

		workerStatus = new HashMap<WorkerInfo, Boolean>();
		for (WorkerInfo worker : mWorkers)
			workerStatus.put(worker, true);

		multipleHolders = new HashMap<Partition, List<WorkerInfo>>();
		for (WorkerInfo worker : mWorkers) {
			List<Partition> partitions = worker.getPartitions();
			// Update each partition entry
			for (Partition p : partitions) {
				if (multipleHolders.containsKey(p))
					multipleHolders.get(p).add(worker);
				else {
					ArrayList<WorkerInfo> list = new ArrayList<WorkerInfo>();
					list.add(worker);
					multipleHolders.put(p, list);
				}
			}
		}
		/* Then we randomly choose one worker for every partition */
		singleHolder = new HashMap<Partition, WorkerInfo>();
		Set<Partition> keySet = multipleHolders.keySet();
		for (Partition p : keySet) {
			List<WorkerInfo> list = multipleHolders.get(p);
			WorkerInfo worker = list.get((new Random()).nextInt(list.size()));
			singleHolder.put(p, worker);
		}
		/* Initialize reduce worker */
		reduceWorkerLoad = new HashMap<WorkerInfo, Set<Integer>>();
		for (WorkerInfo w : mWorkers) {
			Set<Integer> s = new HashSet<Integer>();
			s.add(mWorkers.indexOf(w));
			reduceWorkerLoad.put(w, s);
		}
		resultList = new ArrayList<Result>();
	}

	@Override
	public void run() {
		try {
			/* 1. Get map task and reduce task from the abstract client */
			ObjectInputStream in = new ObjectInputStream(mSocket.getInputStream());
			MapTask mapTask = (MapTask) in.readObject();
			ReduceTask reduceTask = (ReduceTask) in.readObject();

			while (true) {
				ArrayList<Partition> progress = new ArrayList<Partition>();
				Map<WorkerInfo, List<Partition>> workerLoad = new HashMap<WorkerInfo, List<Partition>>();
				for (Partition p : singleHolder.keySet())
					progress.add(p);

				/*
				 * 2. Distribute the {@link MapTask} across a set of
				 * "map-workers" and wait for all map-workers to complete.
				 */
				while (progress.size() != 0) {
					// Delete crashed servers
					for (Partition p : progress) {
						if (!workerStatus.get(singleHolder.get(p))) {
							List<WorkerInfo> wList = multipleHolders.get(p);
							for (WorkerInfo w : wList) {
								if (workerStatus.get(w))
									singleHolder.put(p, w);
							}
						}
					}

					for (Partition p : singleHolder.keySet()) {
						if (!workerLoad.containsKey(singleHolder.get(p))) {
							List<Partition> list = new ArrayList<Partition>();
							list.add(p);
							workerLoad.put(singleHolder.get(p), list);
						} else
							workerLoad.get(singleHolder.get(p)).add(p);
					}

					List<MapCallable> mCallables = new ArrayList<MapCallable>();
					for (WorkerInfo w : workerLoad.keySet()) {
						WorkerCommand mTaskCommand = new MapTaskCommand(mapTask, workerLoad.get(w), w.getName());
						mCallables.add(new MapCallable(mTaskCommand, w, workerStatus));
					}

					/*
					 * 3. At this point we know that all of the map callable
					 * tasks have finished executing, so now we can check the
					 * returned indicator.
					 */
					List<Future<WorkerInfo>> mapResults = null;
					try {
						mapResults = mExecutorWorkerServer.invokeAll(mCallables);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}

					for (Future<WorkerInfo> future : mapResults) {
						try {
							WorkerInfo w = future.get();
							if (w != null) {
								List<Partition> pList = workerLoad.get(w);
								for (Partition p : pList) {
									progress.remove(p);
								}
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
					}
				}

				/*
				 * 4. Now that all map tasks are done, the master server will do
				 * the suffle job. Before this, we do the sanity check for the
				 * fail-safe mode
				 */
				Set<Integer> set = new HashSet<Integer>();
				WorkerInfo failedWorker = null;
				for (WorkerInfo w : reduceWorkerLoad.keySet()) {
					if (!workerStatus.get(w)) {
						set = reduceWorkerLoad.get(w);
						failedWorker = w;
						break;
					}
				}
				if (failedWorker != null) {
					reduceWorkerLoad.remove(failedWorker);
					for (WorkerInfo w : reduceWorkerLoad.keySet()) {
						reduceWorkerLoad.get(w).addAll(set);
						break;
					}
				}

				List<ShuffleCallable> sCallables = new ArrayList<ShuffleCallable>();
				for (WorkerInfo w : workerLoad.keySet()) {
					WorkerCommand sTaskCommand = new ShuffleTaskCommand(w, mWorkers.size(), reduceWorkerLoad.get(w), singleHolder);
					sCallables.add(new ShuffleCallable(sTaskCommand, w, workerStatus));
				}

				/*
				 * 5. At this point we know that all of the shuffle callable
				 * tasks have finished executing, so now we can check the
				 * returned indicator.
				 */
				boolean shuffleFinished = true;
				List<Future<List<WorkerInfo>>> shuffleResults = null;
				try {
					shuffleResults = mExecutorWorkerServer.invokeAll(sCallables);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}

				for (Future<List<WorkerInfo>> future : shuffleResults) {
					try {
						List<WorkerInfo> failedList = future.get();
						if (failedList != null && failedList.size() != 0) {
							for (WorkerInfo w : failedList)
								workerStatus.put(w, false);
							shuffleFinished = false;
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}

				// Restart the task when shuffle failed
				if (!shuffleFinished)
					continue;

				/* 6. Enter the reduce phase */
				List<ReduceCallable> rCallables = new ArrayList<ReduceCallable>();
				for (WorkerInfo w : workerLoad.keySet()) {
					WorkerCommand rTaskCommand = new ReduceTaskCommand(reduceTask, w.getName());
					rCallables.add(new ReduceCallable(rTaskCommand, w, workerStatus));
				}

				/*
				 * 7. At this point we know that all of the reduce callable
				 * tasks have finished executing, so now we can check the
				 * returned indicator.
				 */
				List<Future<Result>> reduceResults = null;
				boolean reduceFinished = true;
				List<Result> partialRes = new ArrayList<Result>();
				try {
					reduceResults = mExecutorWorkerServer.invokeAll(rCallables);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}

				for (Future<Result> future : reduceResults) {
					try {
						Result r = future.get();
						if (r.getFileName() == null) {
							workerStatus.put(r.getWorker(), false);
							reduceFinished = false;
							break;
						} else
							partialRes.add(r);
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
				// Restart the task when reduce failed
				if (!reduceFinished)
					continue;

				// Update the partial result and exit the main process
				resultList.addAll(partialRes);
				break;
			}

			/* 8. Finally, we write back the response to the client server */
			ObjectOutputStream response = null;
			try {
				response = new ObjectOutputStream(mSocket.getOutputStream());
				response.writeObject(resultList);
			} catch (NotSerializableException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					mSocket.close();
				} catch (IOException e) {
				}
			}
		} catch (IOException e) {
			Log.e(TAG, "Connection lost.", e);
		} catch (ClassNotFoundException e) {
			Log.e(TAG, "Received invalid task from client.", e);
		} finally {
			try {
				mExecutorWorkerServer.shutdown();
				mSocket.close();
			} catch (IOException e) {
				// Ignore because we're about to exit anyway.
			}
		}
	}
}