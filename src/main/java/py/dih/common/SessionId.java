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

package py.dih.common;

import py.instance.InstanceId;

public class SessionId {
  private InstanceId initiatorId;

  private InstanceId fellowId;

  public SessionId(InstanceId instanceId, InstanceId fellowId) {
    this.initiatorId = instanceId;
    this.fellowId = fellowId;
  }

  public SessionId(SessionId copyFrom) {
    this.initiatorId = copyFrom.initiatorId;
    this.fellowId = copyFrom.fellowId;
  }


  public InstanceId getInitiatorId() {
    return initiatorId;
  }

  public InstanceId getFellowId() {
    return fellowId;
  }

  @Override
  public String toString() {
    return "SessionID [initiatorID=" + initiatorId + ", fellowID=" + fellowId + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + initiatorId.hashCode();
    result = prime * result + fellowId.hashCode();

    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (null == other) {
      return false;
    } else if (getClass() != other.getClass()) {
      return false;
    } else {
      SessionId sessionId = (SessionId) other;
      return (this.initiatorId.equals(sessionId.initiatorId) && this.fellowId
          .equals(sessionId.fellowId));
    }
  }
}
