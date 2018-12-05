package basic;

import java.io.Serializable;

import basic.basicUtil.WorkerInfo;
/** Used as recording each final result as a file name with the worker that generates it
 * 
 * @author Dingty
 *
 */
public class Result implements Serializable {

	private static final long serialVersionUID = 1650305826427921476L;
	private final WorkerInfo worker;
	private final String fileName;

	public Result(WorkerInfo worker, String fileName) {
		this.fileName = fileName;
		this.worker = worker;
	}

	public String getFileName() {
		return fileName;
	}

	public WorkerInfo getWorker() {
		return worker;
	}

}