package basic.callables;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

import basic.Result;
import basic.basicUtil.WorkerInfo;
import basic.tasks.WorkerCommand;

/**
 * A command that manages a single client-worker connection to be executed
 * asynchronously.
 * 
 * The {@link Callable} interface is similar to {@link Runnable} in that both
 * are commands whose instances may be executed by another thread. Unlike the
 * {@link Runnable#run()} method, however, the {@link Callable#call()} method is
 * able to both (1) return a result and (2) throw a checked exception, so it is
 * a bit more powerful than the {@link Runnable} interface in that regard (in
 * fact, this is mostly why the {@link Callable} interface was introduced in
 * Java 5.0, as described in this blog post:
 * https://blogs.oracle.com/CoreJavaTechTips/entry/get_netbeans_6).
 */
public class ReduceCallable extends GenericCallable<Result> {
	private String fileName;

	public ReduceCallable(WorkerCommand command, WorkerInfo worker, Map<WorkerInfo, Boolean> status) {
		super(command, worker, status);
		fileName = null;
	}

	@Override
	public Result getSuccessResult() {
		return new Result(getWorkers(), fileName);
	}

	@Override
	public Result getFailuredResult() {
		return new Result(getWorkers(), null);
	}

	@Override
	public void interpret(Object result) throws IOException {
		if (!(result instanceof String))
			throw new IOException();
		fileName = (String) result;
	}

}