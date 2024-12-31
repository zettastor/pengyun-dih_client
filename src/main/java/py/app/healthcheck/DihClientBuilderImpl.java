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
