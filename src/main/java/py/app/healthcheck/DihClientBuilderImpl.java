package py.app.healthcheck;

import py.periodic.UnableToStartException;
import py.periodic.WorkerFactory;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;

public class DihClientBuilderImpl implements DihClientBuilder {

  private PeriodicWorkExecutorImpl executor;

  private WorkerFactory dihClientBuildWorkerFactory;

  private int buildRate = 1800000;

  public void setDihClientBuildWorkerFactory(WorkerFactory dihClientBuildWorkerFactory) {
    this.dihClientBuildWorkerFactory = dihClientBuildWorkerFactory;
  }

  public void setBuildRate(int buildRate) {
    this.buildRate = buildRate;
  }

  @Override
  public void startDihClientBuild() throws UnableToStartException {
    ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, buildRate, null);
    executor = new PeriodicWorkExecutorImpl(optionReader,
        dihClientBuildWorkerFactory, "DIH client builder");
    executor.start();
  }

  @Override
  public void stopDihClientBuild() {
    executor.stopNow();
  }
}
