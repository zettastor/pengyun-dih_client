package py.dih.client.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.periodic.Worker;

public class DihClientBuildWorker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(DihClientBuildWorker.class);

  private DihClientFactory dihClientFactory;

  private static DihClientNode rootDihClientNode;

  private File instancesBackupFile;

  private long requestTimeout = 0L;

  public DihClientFactory getDihClientFactory() {
    return dihClientFactory;
  }

  public void setDihClientFactory(DihClientFactory dihClientFactory) {
    this.dihClientFactory = dihClientFactory;
  }

  public static DihClientNode getRootDihClientNode() {
    return rootDihClientNode;
  }

  public void setRootDihClientNode(DihClientNode rootDihClientNode) {
    DihClientBuildWorker.rootDihClientNode = rootDihClientNode;
  }

  public File getInstancesBackupFile() {
    return instancesBackupFile;
  }

  public void setInstancesBackupFile(File instancesBackupFile) {
    this.instancesBackupFile = instancesBackupFile;
  }

  public long getRequestTimeout() {
    return requestTimeout;
  }

  public void setRequestTimeout(long requestTimeout) {
    this.requestTimeout = requestTimeout;
  }


  @Override
  public void doWork() throws Exception {
    List<Instance> instances = getDihInstances();
    buildDihClientNode(instances);

  }

  /**.
   * get DIH instances from local dih client
   * @return DIH instances
   */
  private List<Instance> getDihInstances() {
    Set<Instance> instances = null;
    try {
      DihServiceBlockingClientWrapper localDihClient =
          requestTimeout > 0 ? dihClientFactory.build(rootDihClientNode.getEndPoint(),
              requestTimeout)
              : dihClientFactory.build(rootDihClientNode.getEndPoint());
      instances = localDihClient.getInstances(PyService.DIH.getServiceName(),
          InstanceStatus.HEALTHY);
    } catch (TException e) {
      logger.warn("can not get dih instances from local dih client: {}",
          rootDihClientNode.getEndPoint());
    } catch (GenericThriftClientFactoryException e) {
      logger.warn("can not build connection with dih: {}", rootDihClientNode.getEndPoint());
    }
    if (instances == null || instances.isEmpty()) {
      instances = getDihInstancesByFile();
    }
    return new ArrayList<>(instances);
  }

  /**.
   * build dih client node
   *
   * @param instances dih instances
   */
  private void buildDihClientNode(List<Instance> instances) {
    if (instances == null || instances.isEmpty()) {
      return;
    }
    instances.sort(Comparator.comparing(p -> p.getEndPoint().getHostName()));
    DihClientNode firstNode = null;
    DihClientNode preNode = null;
    for (Instance instance : instances) {
      DihClientNode curNode = null;
      if (instance.getEndPoint().equals(rootDihClientNode.getEndPoint())) {
        curNode = rootDihClientNode;
      } else {
        curNode = new DihClientNode(instance.getEndPoint(), null, requestTimeout);
      }
      if (curNode == null) {
        continue;
      }
      if (firstNode == null) {
        firstNode = curNode;
      } else {
        preNode.setNext(curNode);
      }
      preNode = curNode;
    }
    if (preNode != null) {
      preNode.setNext(firstNode);
    }
  }

  /**.
   * get DIH instances from file
   *
   * @return DIH instances
   */
  private Set<Instance> getDihInstancesByFile() {
    Set<Instance> instances = new HashSet<Instance>();
    BufferedReader reader = null;
    try {
      FileReader fileReader = new FileReader(instancesBackupFile);
      reader = new BufferedReader(fileReader);
      TypeReference<Instance> typeRef = new TypeReference<Instance>() {
      };
      ObjectMapper mapper = new ObjectMapper();

      String line = reader.readLine();
      while (line != null) {
        Instance instance = mapper.readValue(line, typeRef);
        if (PyService.DIH.getServiceName().equals(instance.getName()) && instance.getStatus()
            .equals(InstanceStatus.HEALTHY)) {
          instances.add(instance);
        }
        line = reader.readLine();
      }

      logger.info("The instance " + instances + " read from file");
      reader.close();
    } catch (FileNotFoundException e) {
      logger.warn("failed to get instances due to instance backup file not exists.");
    } catch (IOException e) {
      logger.error("failed to get instances due to failed to read instance from file", e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          logger.warn("can't close file: {} when reading", instancesBackupFile);
        }
      }
    }
    return instances;
  }

  public static class DihClientNode {

    private EndPoint endPoint;

    private DihClientNode next;

    private long requestTimeout = 0L;

    public DihClientNode(EndPoint endPoint,
        DihClientNode next, long requestTimeout) {
      this.endPoint = endPoint;
      this.next = next;
      this.requestTimeout = requestTimeout;
    }

    public EndPoint getEndPoint() {
      return endPoint;
    }

    public void setEndPoint(EndPoint endPoint) {
      this.endPoint = endPoint;
    }

    public DihClientNode getNext() {
      return next;
    }

    public void setNext(DihClientNode next) {
      this.next = next;
    }

    public long getRequestTimeout() {
      return requestTimeout;
    }

    public void setRequestTimeout(long requestTimeout) {
      this.requestTimeout = requestTimeout;
    }

    @Override
    public String toString() {
      DihClientNode curNode = this;
      StringBuilder stringBuilder = new StringBuilder();
      while (curNode != null && curNode.next != this) {
        stringBuilder.append(curNode.getEndPoint().toString()).append("->");
        curNode = curNode.next;
      }
      return stringBuilder.toString();
    }
  }
}
