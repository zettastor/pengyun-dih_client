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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.EndPoint;
import py.dih.common.SessionId;
import py.exception.IllegalIndexException;
import py.instance.DcType;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.Location;
import py.instance.PortType;
import py.thrift.distributedinstancehub.service.DcTypeThrift;
import py.thrift.distributedinstancehub.service.EndPointThrift;
import py.thrift.distributedinstancehub.service.InstanceStatusThrift;
import py.thrift.distributedinstancehub.service.InstanceThrift;
import py.thrift.distributedinstancehub.service.SessionIdThrift;
import py.thrift.share.GroupThrift;

/**
 * Request response of DistributedInstanceHub client.
 *
 * @author liy
 */
public class DihClientRequestResponseHelper {
  private static final Logger logger = LoggerFactory
      .getLogger(DihClientRequestResponseHelper.class);

  public static GroupThrift buildThriftGroupFrom(Group group) {
    if (group == null) {
      return null;
    }

    GroupThrift groupToRemote = new GroupThrift(group.getGroupId());
    return groupToRemote;
  }

  public static Group buildGroupFrom(GroupThrift groupFromRemote) {
    if (groupFromRemote == null) {
      return null;
    }

    Group group = new Group(groupFromRemote.getGroupId());
    return group;
  }

  public static InstanceStatus buildStatusFrom(InstanceStatusThrift status) {
    return InstanceStatus.valueOf(status.name());
  }

  public static InstanceStatusThrift buildThriftStatusFrom(InstanceStatus status) {
    return InstanceStatusThrift.valueOf(status.name());
  }

  public static Instance buildInstanceFrom(InstanceThrift instanceThrift) {
    Instance instance = new Instance(new InstanceId(instanceThrift.getInstanceId()),
        buildGroupFrom(instanceThrift.getGroup()), instanceThrift.getName(),
        buildStatusFrom(instanceThrift.getStatus()));
    for (Entry<Integer, EndPointThrift> entry : instanceThrift.getEndPoints().entrySet()) {
      EndPointThrift endPoint = entry.getValue();
      try {
        instance.putEndPointByServiceName(PortType.get(entry.getKey()),
            new EndPoint(endPoint.getHost(), endPoint.getPort()));
      } catch (IllegalIndexException e) {
        throw new IllegalStateException();
      }
    }

    instance.setChecksum(instanceThrift.getChecksum());
    instance.setHeartBeatCounter(instanceThrift.getHeartBeatCounter());
    if (instanceThrift.getLocation() != null) {
      Location location = null;
      try {
        location = Location.fromString(instanceThrift.getLocation());
      } catch (Exception e) {
        logger.warn("can't parse {} to Location object", instanceThrift.getLocation());
      }

      instance.setLocation(location);
    }
    instance.setNetSubHealth(instanceThrift.isNetSubHealth());
    if (instanceThrift.isSetDcType()) {
      instance.setDcType(DcType.valueOf(instanceThrift.getDcType().name()));
    }
    return instance;
  }

  public static InstanceThrift buildThriftInstanceFrom(Instance instance) {
    InstanceThrift instanceThrift = new InstanceThrift();
    instanceThrift.setInstanceId(instance.getId().getId());
    instanceThrift.setGroup(buildThriftGroupFrom(instance.getGroup()));
    Map<Integer, EndPointThrift> endPoints = new HashMap<Integer, EndPointThrift>();
    for (Entry<PortType, EndPoint> entry : instance.getEndPoints().entrySet()) {
      EndPoint endPoint = entry.getValue();
      endPoints.put(entry.getKey().getValue(),
          new EndPointThrift(endPoint.getHostName(), endPoint.getPort()));
    }

    instanceThrift.setEndPoints(endPoints);
    instanceThrift.setName(instance.getName());
    instanceThrift.setChecksum(instance.getChecksum());
    instanceThrift.setHeartBeatCounter(instance.getHeartBeatCounter());
    instanceThrift.setStatus(buildThriftStatusFrom(instance.getStatus()));
    if (instance.getLocation() != null) {
      instanceThrift.setLocation(instance.getLocation().toString());
    }
    instanceThrift.setNetSubHealth(instance.isNetSubHealth());
    if (instance.getDcType() != null) {
      instanceThrift.setDcType(DcTypeThrift.valueOf(instance.getDcType().name()));
    }
    return instanceThrift;
  }

  public static Group convert(GroupThrift groupFromThrift) {
    Group group = new Group();
    group.setGroupId(groupFromThrift.getGroupId());
    return group;
  }

  public static InstanceStatus convert(InstanceStatusThrift instanceStatusFromThrift) {
    return InstanceStatus.findByValue(instanceStatusFromThrift.getValue());
  }

  public static InstanceId convert(long instanceId) {
    return new InstanceId(instanceId);
  }

  public static SessionIdThrift convert(SessionId sessionId) {
    return new SessionIdThrift(sessionId.getInitiatorId().getId(),
        sessionId.getFellowId().getId());
  }

  public static SessionId convert(SessionIdThrift sessoinIdThrift) {
    return new SessionId(new InstanceId(sessoinIdThrift.getInitiatorId()),
        new InstanceId(sessoinIdThrift.getFellowId()));
  }

}
