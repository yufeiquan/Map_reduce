package basic;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import basic.basicUtil.Partition;
import basic.basicUtil.WorkerInfo;
import basic.tasks.MapTask;
import basic.tasks.ReduceTask;
import basic.util.Log;
import basic.util.StaffUtils;

/**
 * This class represents the "master server" in the distributed map/reduce
 * framework. The {@link MasterServer} is in charge of managing the entire
 * map/reduce computation from beginning to end. The {@link MasterServer}
 * listens for incoming client connections on a distinct host/port address, and
 * is passed an array of {@link WorkerInfo} objects when it is first initialized
 * that provides it with necessary information about each of the available
 * workers in the system (i.e. each worker's name, host address, port number,
 * and the set of {@link Partition}s it stores). A single map/reduce computation
 * managed by the {@link MasterServer} will typically behave as follows:
 * 
 * <ol>
 * <li>Wait for the client to submit a map/reduce task.</li>
 * <li>Distribute the {@link MapTask} across a set of "map-workers" and wait for
 * all map-workers to complete.</li>
 * <li>Distribute the {@link ReduceTask} across a set of "reduce-workers" and
 * wait for all reduce-workers to complete.</li>
 * <li>Write the final key/value pair results of the computation back to the
 * client.</li>
 * </ol>
 */
public class MasterServer extends Thread {
	private final int mPort;
	private final List<WorkerInfo> mWorkers;
	private final ExecutorService mExecutorClient;
	private final ExecutorService mExecutorWorkerServer;

	/** Create at most one thread per available processor on this machine. */
	private static final int POOL_SIZE = 4;
	private static final String TAG = "Worker";

	/**
	 * The {@link MasterServer} constructor.
	 * 
	 * @param masterPort
	 *            The port to listen on.
	 * @param workers
	 *            Information about each of the available workers in the system.
	 */
	public MasterServer(int masterPort, List<WorkerInfo> workers) {
		mPort = masterPort;
		mWorkers = workers;
		mExecutorClient = Executors.newFixedThreadPool(POOL_SIZE);
		mExecutorWorkerServer = Executors.newFixedThreadPool(POOL_SIZE);

	}

	@Override
	public void run() {
		// Basically coping the code from rec13, doing the server job
		try {
			ServerSocket serverSocket = null;
			try {
				serverSocket = new ServerSocket(mPort);
			} catch (IOException e) {
				Log.e(TAG, "Could not open server socket on port " + mPort + ".", e);
				return;
			}

			// Listen to the request from client
			while (true) {
				try {
					Socket clientSocket = serverSocket.accept();
					// Handle the client's request on a background thread, and
					// immediately begin listening for other incoming client
					// connections once again.

					// Do the entire map/reduce computation inside the
					// WorkerCommandHandler#run() method.
					Runnable task = new WorkerCommandHandler(clientSocket, mWorkers, mExecutorWorkerServer);
					mExecutorClient.execute(task);
				} catch (IOException e) {
					Log.e(TAG, "Error while listening for incoming connections.", e);
					break;
				}
			}

			Log.i(TAG, "Shutting down...");

			try {
				serverSocket.close();
			} catch (IOException e) {
				// Ignore because we're about to exit anyway.
			}
		} finally {
			mExecutorClient.shutdown();
		}

	}

	/********************************************************************/
	/***************** STAFF CODE BELOW. DO NOT MODIFY. *****************/
	/********************************************************************/

	/**
	 * Starts the master server on a distinct port. Information about each
	 * available worker in the distributed system is parsed and passed as an
	 * argument to the {@link MasterServer} constructor. This information can be
	 * either specified via command line arguments or via system properties
	 * specified in the <code>master.properties</code> and
	 * <code>workers.properties</code> file (if no command line arguments are
	 * specified).
	 */
	public static void main(String[] args) {
		StaffUtils.makeMasterServer(args).start();
	}

}
