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
