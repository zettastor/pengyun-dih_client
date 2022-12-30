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

import com.fasterxml.jackson.databind.util.StdDateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.icshare.AlarmInfo;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.thrift.distributedinstancehub.service.DistributedInstanceHub;
import py.thrift.distributedinstancehub.service.GetInstanceRequest;
import py.thrift.distributedinstancehub.service.GetInstanceResponse;
import py.thrift.distributedinstancehub.service.GetSyslogRequest;
import py.thrift.distributedinstancehub.service.GetSyslogResponse;
import py.thrift.distributedinstancehub.service.HeartBeatRequest;
import py.thrift.distributedinstancehub.service.HeartBeatResponse;
import py.thrift.distributedinstancehub.service.InstanceThrift;
import py.thrift.distributedinstancehub.service.Syslog;
import py.thrift.distributedinstancehub.service.TurnInstanceToFailedRequest;
import py.thrift.distributedinstancehub.service.TurnInstanceToFailedResponse;
import py.thrift.share.InstanceHasFailedAleadyExceptionThrift;
import py.thrift.share.InstanceNotExistsExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;

/**
 * A class as a wrapper includes some common used remote process call.
 *
 * @author liy
 */
public class DihServiceBlockingClientWrapper {
  private static final Logger logger = LoggerFactory
      .getLogger(DihServiceBlockingClientWrapper.class);

  private final DistributedInstanceHub.Iface delegate;

  public DihServiceBlockingClientWrapper(DistributedInstanceHub.Iface client) {
    delegate = client;
  }

  public DistributedInstanceHub.Iface getDelegate() {
    return this.delegate;
  }

  public void ping() throws TException {
    delegate.ping();

  }

  public HeartBeatResponse heartBeat(HeartBeatRequest request) throws TException {
    return delegate.heartBeat(request);
  }

  public void heartBeat(Instance instance) throws TException {
    HeartBeatRequest request = new HeartBeatRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(DihClientRequestResponseHelper.buildThriftInstanceFrom(instance));
    logger.debug("heart beat request: {},", request);
    heartBeat(request);
  }

  public GetInstanceResponse getInstances(GetInstanceRequest request) throws TException {
    return delegate.getInstances(request);
  }
  

  public Set<Instance> getInstances(String name, InstanceStatus status) throws TException {
    Set<Instance> instances = getInstanceAll();
    Set<Instance> targetInstances = new HashSet<Instance>();

    for (Instance instance : instances) {
      if (name.equals(instance.getName()) && status == instance.getStatus()) {
        targetInstances.add(instance);
      }
    }

    return targetInstances;
  }
  

  public Instance getInstance(long instanceId) throws TException {
    GetInstanceRequest request = new GetInstanceRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstanceId(instanceId);
    GetInstanceResponse response = getInstances(request);
    if (response.getInstanceList().isEmpty()) {
      return null;
    }

    InstanceThrift instanceThrift = response.getInstanceList().get(0);
    try {
      return DihClientRequestResponseHelper.buildInstanceFrom(instanceThrift);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException();
    }
  }

  @Deprecated
  public Instance getInstance(EndPoint endpoint) throws Exception {
    Set<Instance> instances = getInstanceAll();
    for (Instance instance : instances) {
      if (endpoint.equals(instance.getEndPoint())) {
        return instance;
      }
    }

    return null;
  }

  /**
   * Get instance with specified name from dih service.
   *
   * <p>Because of overload when get instance from dih service frequently, it is not suggested to
   * get　instance from dih directly, but to get instance from {@link DihInstanceStore}.(Comment by
   */
  public Set<Instance> getInstance(String name) throws TException {
    Set<Instance> instances = new HashSet<Instance>();
    GetInstanceRequest request = new GetInstanceRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setName(name);
    GetInstanceResponse response = getInstances(request);
    for (InstanceThrift instanceThrift : response.getInstanceList()) {
      try {
        instances.add(DihClientRequestResponseHelper.buildInstanceFrom(instanceThrift));
      } catch (Exception e) {
        logger.error("Caught an exception", e);
        throw new TException();
      }
    }
    return instances;
  }
  

  public Map<InstanceId, Instance> getAllInstances() throws TException {
    Map<InstanceId, Instance> instances = new HashMap<>();
    GetInstanceRequest request = new GetInstanceRequest();
    request.setRequestId(RequestIdBuilder.get());
    GetInstanceResponse response = getInstances(request);
    for (InstanceThrift instanceThrift : response.getInstanceList()) {
      try {
        Instance instance = DihClientRequestResponseHelper.buildInstanceFrom(instanceThrift);
        instances.put(instance.getId(), instance);
      } catch (Exception e) {
        logger.error("Caught an exception", e);
        throw new TException();
      }
    }
    return instances;
  }

  public Set<Instance> getInstanceAll() throws TException {
    Set<Instance> instances = new HashSet<Instance>();
    GetInstanceRequest request = new GetInstanceRequest();
    request.setRequestId(RequestIdBuilder.get());
    GetInstanceResponse response = getInstances(request);
    for (InstanceThrift instanceThrift : response.getInstanceList()) {
      try {
        instances.add(DihClientRequestResponseHelper.buildInstanceFrom(instanceThrift));
      } catch (Exception e) {
        logger.error("Caught an exception", e);
        throw new TException();
      }
    }
    return instances;
  }

  public Set<Instance> getInstanceInGroup(Group group)
      throws ServiceHavingBeenShutdownThrift, TTransportException, TException {
    GetInstanceRequest request = new GetInstanceRequest(RequestIdBuilder.get());
    GetInstanceResponse response = null;
    response = delegate.getInstances(request);

    Set<Instance> instancesInGroup = new HashSet<Instance>();
    for (InstanceThrift instanceFromRemote : response.getInstanceList()) {
      Instance instance = DihClientRequestResponseHelper.buildInstanceFrom(instanceFromRemote);
      if (instance.getGroup() != null && instance.getGroup().equals(group)) {
        instancesInGroup.add(instance);
      }
    }

    return instancesInGroup;
  }

  public TurnInstanceToFailedResponse turnInstanceToFailed(TurnInstanceToFailedRequest request)
      throws InstanceNotExistsExceptionThrift, InstanceHasFailedAleadyExceptionThrift, 
      TException {
    return delegate.turnInstanceToFailed(request);
  }

  public boolean turnInstanceToFailed(long instanceId)
      throws InstanceNotExistsExceptionThrift, InstanceHasFailedAleadyExceptionThrift, 
      TException {
    TurnInstanceToFailedRequest request = new TurnInstanceToFailedRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstanceId(instanceId);
    TurnInstanceToFailedResponse response = turnInstanceToFailed(request);
    if (response != null) {
      logger.info("turn instance {} to failed", instanceId);
      return true;
    }
    return false;
  }

  public void close() {
  }

  public List<AlarmInfo> getSyslog(long lastReportTime) throws TException {
    // logger.debug("Get syslog from time-stamp : {}", Utils.millsecondToString(lastReportTime));
    List<AlarmInfo> alarmInfos = new ArrayList<AlarmInfo>();

    try {
      GetSyslogRequest request = new GetSyslogRequest();
      request.setLastReportTime(lastReportTime);
      GetSyslogResponse response = delegate.getSyslog(request);
      List<Syslog> syslogs = response.getSyslogs();
      for (Syslog syslog : syslogs) {
        AlarmInfo alarmInfo = new AlarmInfo();
        Date date = new Date();

        StdDateFormat dataFormat = new StdDateFormat();
        Date parse;
        try {
          parse = dataFormat.parse(syslog.getTimeStamp());
        } catch (ParseException e) {
          logger.error("Caught an exception when parse the time data", e);
          continue;
        }
        alarmInfo.setTimeStamp(parse.getTime());
        alarmInfo.setSourceObject(syslog.getSourceObject());
        alarmInfo.setDescription(syslog.getDescription());
        alarmInfo.setType(syslog.getType());
        alarmInfo.setLevel(syslog.getLevel());
        alarmInfo.setEndpoint(new EndPoint());
        AlarmInfo.AlarmOper alarmOper = syslog.isAlarmAppear() ? AlarmInfo.AlarmOper.APPEAR
            : AlarmInfo.AlarmOper.DISAPPEAR;
        alarmInfo.setOper(alarmOper);

        alarmInfos.add(alarmInfo);
      }

      logger.debug("All alarm informations are : {}", alarmInfos);
      return alarmInfos;
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new TException();
    }

  }
}
