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

package py.icshare;

import py.common.struct.EndPoint;

public class AlarmInfo {
  private EndPoint endpoint;
  private String sourceObject;
  private long timeStamp;
  private String description;
  private String level;
  private String type;
  private AlarmOper oper;
  private long times;

  public EndPoint getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(EndPoint endpoint) {
    this.endpoint = endpoint;
  }

  public String getSourceObject() {
    return sourceObject;
  }

  public void setSourceObject(String sourceObject) {
    this.sourceObject = sourceObject;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public long getTimes() {
    return times;
  }

  public void setTimes(long times) {
    this.times = times;
  }

  public AlarmOper getOper() {
    return oper;
  }

  public void setOper(AlarmOper oper) {
    this.oper = oper;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((endpoint == null) ? 0 : endpoint.hashCode());
    result = prime * result + ((level == null) ? 0 : level.hashCode());
    result = prime * result + ((sourceObject == null) ? 0 : sourceObject.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    AlarmInfo other = (AlarmInfo) obj;
    if (endpoint == null) {
      if (other.endpoint != null) {
        return false;
      }
    } else if (!endpoint.equals(other.endpoint)) {
      return false;
    }
    if (level == null) {
      if (other.level != null) {
        return false;
      }
    } else if (!level.equals(other.level)) {
      return false;
    }
    if (sourceObject == null) {
      if (other.sourceObject != null) {
        return false;
      }
    } else if (!sourceObject.equals(other.sourceObject)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "AlarmInfo [endpoint=" + endpoint + ", sourceObject=" + sourceObject + ", timeStamp="
        + /*Utils.millsecondToString(timeStamp) +*/ ", description=" + description + ", level="
        + level
        + ", type=" + type + ", oper=" + oper + ", times=" + times + "]";
  }

  public enum AlarmOper {
    APPEAR, DISAPPEAR
  }

}
