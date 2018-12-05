package basic;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import basic.plugin.wordcount.WordCountClient;
import basic.plugin.wordprefix.WordPrefixClient;
import basic.tasks.MapTask;
import basic.tasks.ReduceTask;

/**
 * An abstract client class used primarily for code reuse between the
 * {@link WordCountClient} and {@link WordPrefixClient}.
 */
public abstract class AbstractClient {
	private final String mMasterHost;
	private final int mMasterPort;

	/**
	 * The {@link AbstractClient} constructor.
	 * 
	 * @param masterHost
	 *            The host name of the {@link MasterServer}.
	 * @param masterPort
	 *            The port that the {@link MasterServer} is listening on.
	 */
	public AbstractClient(String masterHost, int masterPort) {
		mMasterHost = masterHost;
		mMasterPort = masterPort;
	}

	protected abstract MapTask getMapTask();

	protected abstract ReduceTask getReduceTask();

	public void execute() {
		final MapTask mapTask = getMapTask();
		final ReduceTask reduceTask = getReduceTask();

		// TODO: Submit the map/reduce task to the master and wait for the task
		// to complete.
		Socket socket = null;
		ObjectInputStream in = null;
		try {
			socket = new Socket(mMasterHost, mMasterPort);
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(mapTask);
			out.writeObject(reduceTask);
			// Wait for the master server to return result.
			in = new ObjectInputStream(socket.getInputStream());
			ArrayList<Result> containerList = (ArrayList<Result>) in.readObject();
			// Master server returns the correct result
			if (containerList.size() == 0) {
				System.err.println("The map-reduce process fails!");
			} else {
				System.out.println("The map-reduce process is successful!");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// wait for the task to complete, return paths to the multiple reduced
		// result should be ok...
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
