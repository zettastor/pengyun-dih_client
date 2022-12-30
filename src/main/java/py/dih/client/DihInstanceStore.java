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

import com.google.common.collect.ImmutableBiMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;

/**
 * An instancestore implements InstanceStore that gets instances from DIH. In order to make
 * instances correctly we use a periodic worker to get instances from DIH
 *
 * <p>This instance store is immutable in that no one except the periodic worker can change its
 * content. Moreover, it is unnecessary to synchronize this class's method because it is immutable.
 *
 * <p>Considering the common case which is getting an instance by its id, we use map to store
 * instance
 * records.
 *
 * @author liy
 */
public class DihInstanceStore implements InstanceStore {
  private static final Logger logger = LoggerFactory.getLogger(DihInstanceStore.class);
  PeriodicWorkExecutorImpl executor = null;
  private EndPoint dihEndPoint;
  private int refreshRate = 10000; // every 10 seconds
  private long requestTimeout = 20000; // default timeout 20 seconds
  private AtomicReference<ImmutableBiMap<InstanceId, Instance>> instanceMapRef;
  private boolean hasBeenInitialized = false;

  /**
   * when dih restart, the worker will get newly instances from dih. but instance kept older
   * instance as well. so it needs to record the instance in older but not in newly. after 3 times
   * refreshing, the older instance still not in newly, then not put it in instancestore.
   */
  private Map<Long, Integer> garbageMap = new ConcurrentHashMap<Long, Integer>();

  private DihClientFactory dihClientFactory;

  private DihInstanceStore() /* throws Exception */ {
    this.instanceMapRef = new AtomicReference<ImmutableBiMap<InstanceId, Instance>>();
    ImmutableBiMap.Builder<InstanceId, Instance> builder 
        = new ImmutableBiMap.Builder<InstanceId, Instance>();
    instanceMapRef.set(builder.build());
  }

  public static DihInstanceStore getSingleton() {
    return LazyHolder.singletonInstance;
  }

  public DihClientFactory getDihClientFactory() {
    return dihClientFactory;
  }

  public void setDihClientFactory(DihClientFactory dihClientFactory) {
    this.dihClientFactory = dihClientFactory;
  }

  public Set<Instance> getInstances() {
    return instanceMapRef.get().values();
  }

  public void setInstances(Set<Instance> instances) {
    throw new RuntimeException("Save operation is not supported");
  }

  public int getRefreshRate() {
    return refreshRate;
  }

  public void setRefreshRate(int refreshRate) {
    this.refreshRate = refreshRate;
  }

  public long getRequestTimeout() {
    return requestTimeout;
  }

  public void setRequestTimeout(long requestTimeout) {
    this.requestTimeout = requestTimeout;
  }

  public void init() throws Exception {
    if (!hasBeenInitialized) {
      // logger.debug("The {}",);
      executor = new PeriodicWorkExecutorImpl(null, null, "pull-instance-from-dih");
      ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, refreshRate, null);
      executor.setWorkerFactory(new WorkerFactory() {
        @Override
        public Worker createWorker() {
          return new RetrieveInstancesWorker();
        }
      });
      executor.setExecutionOptionsReader(optionReader);
      executor.start();
    }

    hasBeenInitialized = true;
  }

  @Override
  public void close() {
    logger.warn("close the instance retriever", new Exception());
    if (executor != null) {
      executor.stop();
    }
  }

  @Override
  public void save(Instance instance) {
    throw new RuntimeException("Save operation is not supported");
  }

  @Override
  public Set<Instance> getAll(String name, InstanceStatus status) {
    Set<Instance> returnedSet = new HashSet<Instance>();
    logger.debug("Got all instance from instance store: {}", instanceMapRef.get().values());
    for (Instance instance : instanceMapRef.get().values()) {
      if (name.equals(instance.getName()) && status.equals(instance.getStatus())) {
        returnedSet.add(instance);
      }
    }
    return returnedSet;
  }

  @Override
  public Set<Instance> getAll(InstanceStatus status) {
    Set<Instance> returnedSet = new HashSet<Instance>();
    for (Instance instance : instanceMapRef.get().values()) {
      if (status.equals(instance.getStatus())) {
        returnedSet.add(instance);
      }
    }

    return returnedSet;
  }

  @Override
  public Set<Instance> getAll(String name) {
    Set<Instance> returnedSet = new HashSet<Instance>();
    for (Instance instance : instanceMapRef.get().values()) {
      if (name.equals(instance.getName())) {
        returnedSet.add(instance);
      }
    }

    return returnedSet;
  }

  @Override
  public Set<Instance> getAll() {
    return instanceMapRef.get().values();
  }

  @Override
  public Instance get(EndPoint endPoint) {
    for (Instance instance : instanceMapRef.get().values()) {
      for (EndPoint ep : instance.getEndPoints().values()) {
        if (ep.equals(endPoint)) {
          return instance;
        }
      }
    }
    return null;
  }

  @Override
  public Instance get(InstanceId id) {
    return instanceMapRef.get().get(id);
  }

  @Override
  public void delete(Instance instance) {
    throw new RuntimeException("Save operation is not supported");
  }

  public EndPoint getDihEndPoint() {
    return dihEndPoint;
  }

  public void setDihEndPoint(EndPoint dihEndPoint) {
    this.dihEndPoint = dihEndPoint;
  }

  @Override
  public Instance getByHostNameAndServiceName(String hostName, String name) {
    for (Instance instance : instanceMapRef.get().values()) {
      for (EndPoint ep : instance.getEndPoints().values()) {
        if (ep.getHostName().equals(hostName) && instance.getName().equals(name)) {
          return instance;
        }
      }
    }
    return null;
  }

  private static class LazyHolder {
    private static final DihInstanceStore singletonInstance = new DihInstanceStore();
  }

  class RetrieveInstancesWorker implements Worker {
    private static final int REFRESHING_TIME_UPPER_BOUND = 3;

    public List<EndPoint> getActiveDihList() {
      Set<Instance> instanceSets = getAll(PyService.DIH.getServiceName(), InstanceStatus.HEALTHY);
      List<EndPoint> endPointList = new ArrayList<>();
      logger.debug("getActiveDIH instances {}", instanceSets);
      for (Instance instance : instanceSets) {
        if (!dihEndPoint.equals(instance.getEndPoint())) {
          endPointList.add(instance.getEndPoint());
        }
      }
      return endPointList;
    }

    public void doWork() throws Exception {
      boolean isException = false;
      ImmutableBiMap.Builder<InstanceId, Instance> newInstanceMapBuilder 
          = new ImmutableBiMap.Builder<InstanceId, Instance>();

      try {
        DihServiceBlockingClientWrapper client = dihClientFactory
            .build(dihEndPoint, requestTimeout);
        for (Instance instance : client.getInstanceAll()) {
          logger.debug("Got instance from DIH: {}", instance);
          newInstanceMapBuilder.put(instance.getId(), instance);
        }
      } catch (Exception e) {
        List<EndPoint> endpoints = getActiveDihList();
        logger.warn(
            "Caught an exception when retrieve info of instances from {}, " 
                + "msg {} active endpoint {}",
            dihEndPoint, e.getMessage(), endpoints);
        if (endpoints != null) {
          isException = true;
          for (EndPoint ep : endpoints) {
            if (!isException) {
              break;
            }
            try {
              isException = false;
              DihServiceBlockingClientWrapper client = dihClientFactory.build(ep, requestTimeout);
              for (Instance instance : client.getInstanceAll()) {
                logger.debug("Got instance from DIH: {}", instance);
                newInstanceMapBuilder.put(instance.getId(), instance);
              }
            } catch (Exception e1) {
              isException = true;
              logger.warn("Caught Exception e1 ep {}", ep);
            }
          }
        }
      } finally {
        logger.debug("finally process");
        int refreshingTimes = 0;
        ImmutableBiMap<InstanceId, Instance> toMergingInstanceMap = newInstanceMapBuilder.build();
        /*
         * Give old instance 3 chances to keep existing in local instance store if we cannot get it 
         * from remote,
         * no matter it being deleted or something wrong to get it. Once chances lost, remove 
         * the instance from
         * local instance store.
         */
        for (Instance instance : instanceMapRef.get().values()) {
          Instance newInstance = toMergingInstanceMap.get(instance.getId());
          if (newInstance != null) {
            continue;
          }
          if (isException) {
            // When any exception occurred after round-robin query, keep current state of all .
            // instances in
            // store.
            newInstanceMapBuilder.put(instance.getId(), instance);
            continue;
          }

          if (garbageMap.get(instance.getId().getId()) == null) {
            refreshingTimes = 0;
            garbageMap.put(instance.getId().getId(), 0);
          } else {
            refreshingTimes = garbageMap.get(instance.getId().getId());
            garbageMap.put(instance.getId().getId(), ++refreshingTimes);
          }
          if (refreshingTimes < REFRESHING_TIME_UPPER_BOUND) {
            newInstanceMapBuilder.put(instance.getId(), instance);
          } else {
            garbageMap.remove(instance.getId().getId());
          }
        }
      }

      instanceMapRef.set(newInstanceMapBuilder.build());
    }
  }
}
