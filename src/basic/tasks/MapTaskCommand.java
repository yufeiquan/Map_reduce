package basic.tasks;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;

import com.sun.jmx.snmp.tasks.Task;

import basic.basicUtil.Emitter;
import basic.basicUtil.EmitterImpl;
import basic.basicUtil.Partition;
import basic.util.WorkerStorage;

/**
 * A {@link WorkerCommand} that executes a {@link Task} and sends the calculated
 * result back to the client.
 * 
 * Note that the generic type <code>T</code> must extend {@link Serializable}
 * since we will be writing it over the network back to the client using an
 * {@link ObjectOutputStream}.
 */
public class MapTaskCommand extends WorkerCommand {
	private static final long serialVersionUID = -5314076333098679665L;

	private final MapTask mTask;
	private final List<Partition> mFileName;
	private final String workerName;

	public MapTaskCommand(MapTask task, List<Partition> mFileName, String workerName) {
		this.mTask = task;
		this.mFileName = mFileName;
		this.workerName = workerName;
	}

	@Override
	public void run() {
		// Get the socket to use to send results back to the client. Note that
		// the WorkerServer will close this socket for us, so we don't need to
		// worry about that.
		Socket socket = getSocket();
		ObjectOutputStream out = null;
		FileInputStream in = null;
		String workerServerIntemediateName = WorkerStorage.getIntermediateResultsDirectory(workerName);
		String workerServerOutFileName = "";
		// Iterate through the files in each partition
		for (Partition p : mFileName) {
			Iterator<File> fileIter = p.iterator();
			workerServerOutFileName = workerServerIntemediateName + "\\" + p.getPartitionName() + ".txt";
			FileInputStream exist = null;
			try {
				exist = new FileInputStream(workerServerOutFileName);
			} catch (FileNotFoundException e1) {
				while (fileIter.hasNext()) {
					try {
						in = new FileInputStream(fileIter.next());
						Emitter emitter = new EmitterImpl(new File(workerServerOutFileName));
						mTask.execute(in, emitter);
					} catch (IOException e2) {
						System.err.println("Open file filed at worker:" + workerName);
						e2.printStackTrace();
					} finally {
						try {
							if (in != null) {
								in.close();
							}
						} catch (IOException e3) {
							System.err.println("Close file error");
							e3.printStackTrace();
						}
					}
				}
			} finally {
				if (exist != null) {
					try {
						exist.close();
					} catch (IOException e4) {
						e4.printStackTrace();
					}
				}
			}
		}
		// Write back the result, 1 indicates that the process is successful
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(new Integer(1));
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
