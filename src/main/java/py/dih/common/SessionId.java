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
