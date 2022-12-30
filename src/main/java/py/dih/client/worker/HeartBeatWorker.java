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

import java.util.Map;
import java.util.Map.Entry;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;
import py.instance.DcType;
import py.instance.Instance;
import py.instance.PortType;
import py.periodic.Worker;

public class HeartBeatWorker implements Worker {
  private static final Logger logger = LoggerFactory.getLogger(HeartBeatWorker.class);

  private DihClientFactory dihClientFactory;

  private AppContext appContext;

  private EndPoint localDihEndPoint;

  private long requestTimeout = 0L;

  private boolean netSubHealth = false;

  private DcType dcType = DcType.NORMALSUPPORT;

  public boolean isNetSubHealth() {
    return netSubHealth;
  }

  public void setNetSubHealth(boolean netSubHealth) {
    this.netSubHealth = netSubHealth;
  }

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

  public void setDcType(DcType type) {
    this.dcType = type;
  }

  @Override
  public void doWork() throws Exception {
    Map<PortType, EndPoint> endPoints = appContext.getEndPoints();
    if (endPoints.isEmpty()) {
      logger.error("there is no service on instance: " + appContext);
      return;
    }

    Instance instance = new Instance(appContext.getInstanceId(), appContext.getGroup(),
        appContext.getLocation(),
        appContext.getInstanceName(), appContext.getStatus());
    for (Entry<PortType, EndPoint> entry : endPoints.entrySet()) {
      instance.putEndPointByServiceName(entry.getKey(), entry.getValue());
    }
    instance.setNetSubHealth(netSubHealth);
    instance.setDcType(dcType);

    logger.debug("heartbeat with dih, instance: {}", instance);
    DihServiceBlockingClientWrapper client = null;
    try {
      client = requestTimeout > 0 ? dihClientFactory.build(localDihEndPoint, requestTimeout)
          : dihClientFactory.build(localDihEndPoint);
    } catch (Exception e) {
      logger.warn("can not build connection with dih: {}", localDihEndPoint);
      return;
    }

    try {
      client.heartBeat(instance);
    } catch (TException e) {
      logger.error("Heart Beat Exception catch ", e);
    }
  }
}
