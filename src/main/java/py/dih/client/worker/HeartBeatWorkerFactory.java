/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
