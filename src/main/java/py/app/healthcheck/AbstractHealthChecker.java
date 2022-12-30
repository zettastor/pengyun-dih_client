/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.app.healthcheck;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.dih.client.worker.HeartBeatWorkerFactory;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;

/**
 * AbstractHealthChecker.
 */
public abstract class AbstractHealthChecker implements HealthChecker {
  protected static final String SERVICE_HEALTH_CHECK_METHOD_NAME = "ping";
  protected static final int DEFAULT_CHECKING_RATE = 1000; // ms
  private static final Logger logger = LoggerFactory.getLogger(AbstractHealthChecker.class);
  protected List<Object> serviceClients;
  protected List<Method> methods;
  protected AppContext appContext;
  // Setters
  private int checkingRate;
  // Internal variables
  private PeriodicWorkExecutorImpl executor;
  private WorkerFactory heartBeatWorkerFactory;
  private boolean netSubHealth;

  /**
   * AbstractHealthChecker.
   *
   * @param checkingRate checkingRate
   * @param appContext   appContext
   */
  public AbstractHealthChecker(int checkingRate, AppContext appContext) {
    super();
    this.methods = new ArrayList<>();
    this.serviceClients = new ArrayList<>();
    this.checkingRate = checkingRate;
    this.appContext = appContext;
  }

  @Override
  public void startHealthCheck() throws Exception {
    initHeartbeatItselfFactory();

    WorkerFactory workerFactory = HealthCheckWorker::new;
    ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, checkingRate, null);
    executor = new PeriodicWorkExecutorImpl(optionReader, workerFactory, "health checker");
    executor.start();
  }

  @Override
  public void stopHealthCheck() {
    // Stop the executor immediately. No meaning to wait
    executor.stopNow();
  }

  public void setCheckingRate(int checkingRate) {
    this.checkingRate = checkingRate;
  }

  public void setHeartBeatWorkerFactory(WorkerFactory heartBeatWorkerFactory) {
    this.heartBeatWorkerFactory = heartBeatWorkerFactory;
  }

  public void setNetSubHealth(boolean netSubHealth) {
    this.netSubHealth = netSubHealth;
  }

  protected abstract boolean isServiceHealthy() throws Exception;

  protected abstract void initHeartbeatItselfFactory() throws Exception;

  private void checkServiceHealthAndSendHeartBeat() throws Exception {
    if (isServiceHealthy() && heartBeatWorkerFactory != null) {
      try {
        Worker heartBeatWorker = heartBeatWorkerFactory.createWorker();

        if (HeartBeatWorkerFactory.class.isInstance(heartBeatWorkerFactory)) {
          ((HeartBeatWorkerFactory) heartBeatWorkerFactory).setNetSubHealth(netSubHealth);
        }

        heartBeatWorker.doWork();
      } finally {
        logger.info("nothing need to do here");
      }
    }
  }

  private class HealthCheckWorker implements Worker {
    @Override
    public void doWork() throws Exception {
      checkServiceHealthAndSendHeartBeat();
    }
  }
}
