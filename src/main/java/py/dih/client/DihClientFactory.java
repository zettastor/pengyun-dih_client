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

package py.dih.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.exception.GenericThriftClientFactoryException;
import py.thrift.distributedinstancehub.service.DistributedInstanceHub;

public class DihClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(DihClientFactory.class);
  private static final int DEFAULT_REQUEST_TIMEOUT_MS = 20000; // 20seconds
  private GenericThriftClientFactory<DistributedInstanceHub.Iface> genericClientFactory;

  public DihClientFactory() {
    genericClientFactory = GenericThriftClientFactory.create(DistributedInstanceHub.Iface.class);
  }

  public DihClientFactory(int minWorkThreadCount) {
    genericClientFactory = GenericThriftClientFactory.create(DistributedInstanceHub.Iface.class,
        minWorkThreadCount);
  }

  public DihClientFactory(int minWorkThreadCount, int connectionTimeoutMs) {
    genericClientFactory = GenericThriftClientFactory
        .create(DistributedInstanceHub.Iface.class, minWorkThreadCount)
        .withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  public DihClientFactory(int minWorkThreadCount, int maxWorkThreadCount, int connectionTimeoutMs) {
    genericClientFactory = GenericThriftClientFactory
        .create(DistributedInstanceHub.Iface.class, minWorkThreadCount, maxWorkThreadCount)
        .withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  public DihServiceBlockingClientWrapper build(EndPoint eps)
      throws GenericThriftClientFactoryException {
    return build(eps, DEFAULT_REQUEST_TIMEOUT_MS);
  }

  public DihServiceBlockingClientWrapper build(EndPoint eps, long requestTimeout)
      throws GenericThriftClientFactoryException {
    if (eps == null) {
      return null;
    }

    DistributedInstanceHub.Iface client = genericClientFactory
        .generateSyncClient(eps, requestTimeout);
    return new DihServiceBlockingClientWrapper(client);
  }

  public void close() {
    if (genericClientFactory != null) {
      genericClientFactory.close();
      genericClientFactory = null;
    }
  }

  public GenericThriftClientFactory<DistributedInstanceHub.Iface> getGenericClientFactory() {
    return genericClientFactory;
  }

}
