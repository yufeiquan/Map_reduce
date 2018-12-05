package basic.callables;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.Callable;

import basic.basicUtil.WorkerInfo;
import basic.tasks.WorkerCommand;

public abstract class GenericCallable<T> implements Callable<T> {
	private final WorkerCommand workerCommand;
	private final WorkerInfo workerInfo;
	private final Map<WorkerInfo, Boolean> wStatus;

	public GenericCallable(WorkerCommand command, WorkerInfo worker, Map<WorkerInfo, Boolean> status) {
		workerCommand = command;
		workerInfo = worker;
		wStatus = status;
	}

	/**
	 * Returns the {@link WorkerInfo} list that the Callable task is interacting with
	 */
	public WorkerInfo getWorkers() {
		return workerInfo;
	}

	@Override
	public T call() throws Exception {
		Socket socket = null;
		try {
			// Establish a connection with the worker server.
			socket = new Socket(workerInfo.getHost(), workerInfo.getPort());

			// Create the ObjectOutputStream and write the WorkerCommand
			// over the network to be read and executed by a WorkerServer.
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(workerCommand);
			out.flush();

			// Note that we instantiate the ObjectInputStream AFTER writing
			// the object over the objectOutputStream. Initializing it
			// immediately after initializing the ObjectOutputStream (but
			// before writing the object) will cause the entire program to
			// block, as described in this StackOverflow answer:
			// http://stackoverflow.com/q/5658089/844882:
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

			// Read and return the worker's final result.
			Object result = in.readObject();
			interpret(result);
		} catch (IOException e) {
			System.out.println("SendWorker: " + workerInfo.getName() + " crashes.");
			wStatus.put(workerInfo, false);
			return getFailuredResult();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				if (socket != null) {
					socket.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return getSuccessResult();
	}

	public abstract void interpret(Object result) throws IOException;

	public abstract T getSuccessResult();
 
	public abstract T getFailuredResult();
}
