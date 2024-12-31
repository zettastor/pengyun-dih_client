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

import java.io.File;
import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.dih.client.worker.DihClientBuildWorker.DihClientNode;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

/**
 * This factory is not thread safe. It is expected that the factory is a singleton object injected
 * by spring.
 *
 * @author chenlia
 */
public class DihClientBuildWorkerFactory implements WorkerFactory {

  private static DihClientBuildWorker worker = null;

  private DihClientFactory dihClientFactory;

  private EndPoint localDihEndPoint;

  private long requestTimeout = 0;

  private String instancesBackupPath = "var/Instance_Backup";

  public EndPoint getLocalDihEndPoint() {
    return localDihEndPoint;
  }

  public void setLocalDihEndPoint(EndPoint localDihEndPoint) {
    this.localDihEndPoint = localDihEndPoint;
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

  public String getInstancesBackupPath() {
    return instancesBackupPath;
  }

  public void setInstancesBackupPath(String instancesBackupPath) {
    this.instancesBackupPath = instancesBackupPath;
  }

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new DihClientBuildWorker();
      worker.setRootDihClientNode(new DihClientNode(localDihEndPoint, null, requestTimeout));
      worker.setRequestTimeout(requestTimeout);
      worker.setDihClientFactory(dihClientFactory);
      worker.setInstancesBackupFile(new File(instancesBackupPath));
    }
    return worker;
  }
}
