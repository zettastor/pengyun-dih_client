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

package py.app.healthcheck;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.instance.PortType;

/**
 * check instance health periodically.
 */
public class HealthCheckerWithThriftImpl extends AbstractHealthChecker {
  private static final Logger logger = LoggerFactory.getLogger(HealthCheckerWithThriftImpl.class);

  // Setters
  private Class<?> serviceClientClazz;

  // Internal variables
  private GenericThriftClientFactory<?> genericThriftClientFactory;

  public HealthCheckerWithThriftImpl(AppContext appContext) {
    this(DEFAULT_CHECKING_RATE, appContext);
  }

  public HealthCheckerWithThriftImpl(int checkingRate, AppContext appContext) {
    super(checkingRate, appContext);
  }

  /**
   * Construct client object. Always health check a service at the local host.
   *
   * <p>Wrap the TSocket with TFramedTransport so that the client can correctly communicate with
   * nonblocking server
   *
   * <p>TODO: we assume a service is always using nonblocking way to communicate. We need to
   * specify this in some configuration file.
   */
  protected void initHeartbeatItselfFactory() throws Exception {
    Map<PortType, EndPoint> endPoints = appContext.getEndPoints();
    if (serviceClientClazz == null || endPoints.size() == 0) {
      throw new Exception("ServiceClientClass or service port are not set yet");
    }

    genericThriftClientFactory = GenericThriftClientFactory.create(serviceClientClazz, 1);
    // set defaultSocketTimeoutMs = 10000, defaultConnectionTimeoutMs = 10000 
    // cause we find sometimes connect to service will timeout
    Object serviceClient = genericThriftClientFactory
        .generateSyncClient(appContext.getMainEndPoint(), 10000, 10000);
    validateServiceHealthCheckMethodExists(serviceClient);
    Method method = serviceClient.getClass().getMethod(SERVICE_HEALTH_CHECK_METHOD_NAME);
    methods.add(method);
    serviceClients.add(serviceClient);
  }

  @Override
  public void stopHealthCheck() {
    super.stopHealthCheck();
    if (genericThriftClientFactory != null) {
      genericThriftClientFactory.close();
      genericThriftClientFactory = null;
    }
  }

  public void setServiceClientClazz(Class<?> serviceClientClazz) {
    this.serviceClientClazz = serviceClientClazz;
  }

  @Override
  protected boolean isServiceHealthy() throws Exception {
    try {
      /*Only control Channel is considered, there is only one service client.*/
      Validate.isTrue(serviceClients.size() == 1, "Expect only one client.");
      for (int i = 0; i < serviceClients.size(); i++) {
        /*Modified by Vin xu for DataNode's status couldn't 
        recovered after network been recovered at 2017-3-21 begin.*/
        try {
          methods.get(i).invoke(serviceClients.get(i));
        } catch (InvocationTargetException e) {
          /*if socket failed create a new socket. */
          if (e.getTargetException() instanceof TTransportException) {
            serviceClients.set(i,
                genericThriftClientFactory.generateSyncClient(appContext.getMainEndPoint()));
            logger.error(
                "the service fails to call the method ping(), and now create a socket again," 
                    + " endpoint={}",
                appContext.getMainEndPoint());

            /*If control channel only be considered continue in following is needed.*/
            return false;

          } else {
            logger.error("the service fails to call the method ping()", e.getTargetException());
            throw e;
          }

        }
        /*Modified by Vin xu for DataNode's status couldn't recovered after network been recovered 
        at 2017-3-21 end .*/
      }
      return true;
    } catch (SecurityException | IllegalArgumentException | IllegalAccessException e) {
      logger.error("the service doesn't have ping() method defined", e);
      throw e;
    } catch (Exception e) {
      if (e instanceof IOException) {
        logger.info("Can't ping the service which might be unhealthy", e);
        return false;
      } else if (e instanceof TException) {
        logger.info("service returns unexpected exception", e);
        return false;
      } else {
        logger.info("Unknown exception", e);
        throw e;
      }
    } finally {
      logger.info("nothing need to do here");
    }
  }

  protected void validateServiceHealthCheckMethodExists(Object clientObject) throws Exception {
    try {
      clientObject.getClass().getMethod(SERVICE_HEALTH_CHECK_METHOD_NAME);
    } catch (SecurityException | NoSuchMethodException e) {
      logger.error("the service doesn't have heath check method defined", e);
      throw e;
    } catch (Exception e) {
      logger.error("something wrong with the client object", e);
      throw e;
    }
  }
}