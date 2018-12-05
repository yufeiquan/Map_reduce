package basic.callables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import basic.basicUtil.WorkerInfo;
import basic.tasks.WorkerCommand;

public class ShuffleCallable extends GenericCallable<List<WorkerInfo>> {

	private final List<WorkerInfo> wFailed;

	public ShuffleCallable(WorkerCommand command, WorkerInfo worker, Map<WorkerInfo, Boolean> status) {
		super(command, worker, status);
		wFailed = new ArrayList<WorkerInfo>();
	}

	@Override
	public List<WorkerInfo> getSuccessResult() {
		return wFailed;
	}

	@Override
	public List<WorkerInfo> getFailuredResult() {
		return wFailed;
	}

	@Override
	public void interpret(Object result) throws IOException {
		wFailed.addAll((ArrayList<WorkerInfo>) result);
	}

}
