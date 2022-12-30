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

package py.dih.client.worker;

import py.app.context.AppContext;
import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.instance.DcType;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

/**
 * This factory is not thread safe. It is expected that the factory is a singleton object injected
 * by spring.
 *
 * @author chenlia
 */
public class HeartBeatWorkerFactory implements WorkerFactory {
  private static HeartBeatWorker worker = null;

  private DihClientFactory dihClientFactory;

  private AppContext appContext;

  private EndPoint localDihEndPoint;

  private long requestTimeout = 0;

  private DcType dcType;

  public EndPoint getLocalDihEndPoint() {
    return localDihEndPoint;
  }

  public void setLocalDihEndPoint(EndPoint localDihEndPoint) {
    this.localDihEndPoint = localDihEndPoint;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public DihClientFactory getDihClientFactory() {
    return dihClientFactory;
  }

  public void setDihClientFactory(DihClientFactory dihClientFactory) {
    this.dihClientFactory = dihClientFactory;
  }

  public long getRequestTimeout() {
    return requestTimeout;
  }

  public void setRequestTimeout(long requestTimeout) {
    this.requestTimeout = requestTimeout;
  }

  public DcType getDcType() {
    return dcType;
  }

  public void setDcType(DcType dcType) {
    this.dcType = dcType;
  }

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new HeartBeatWorker();
      worker.setAppContext(appContext);
      worker.setLocalDihEndPoint(localDihEndPoint);
      worker.setRequestTimeout(requestTimeout);
      worker.setDihClientFactory(dihClientFactory);
      worker.setDcType(dcType);
    }
    return worker;
  }

  public void setNetSubHealth(boolean netSubHealth) {
    worker.setNetSubHealth(netSubHealth);
  }
}
