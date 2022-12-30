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

import io.netty.buffer.ByteBufAllocator;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.struct.EndPoint;
import py.instance.PortType;
import py.netty.core.MethodCallback;
import py.netty.exception.DisconnectionException;

/**
 * GenerateHealthCheckerImpl.
 *
 * @param <T> T
 */
public class GenerateHealthCheckerImpl<T> extends AbstractHealthChecker {
  private static final Logger logger = LoggerFactory.getLogger(AbstractHealthChecker.class);
  private final HealthCheckerClientFactory<T> healthCheckerClientFactory;

  public GenerateHealthCheckerImpl(int checkingRate, AppContext appContext,
      HealthCheckerClientFactory<T> healthCheckerClientFactory) {
    super(checkingRate, appContext);
    this.healthCheckerClientFactory = healthCheckerClientFactory;
  }

  public GenerateHealthCheckerImpl(AppContext appContext,
      HealthCheckerClientFactory<T> healthCheckerClientFactory) {
    super(DEFAULT_CHECKING_RATE, appContext);
    this.healthCheckerClientFactory = healthCheckerClientFactory;
  }

  @Override
  protected boolean isServiceHealthy() throws Exception {
    Validate.isTrue(serviceClients.size() == 1, "Expect only one client.");
    Method method = methods.get(0);
    Object client = serviceClients.get(0);
    try {
      try {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicBoolean result = new AtomicBoolean(false);
        MethodCallback<Object> callback = new MethodCallback<Object>() {
          @Override
          public void complete(Object object) {
            result.set(true);
            countDownLatch.countDown();
          }

          @Override
          public void fail(Exception e) {
            result.set(false);
            countDownLatch.countDown();
          }

          @Override
          public ByteBufAllocator getAllocator() {
            return null;
          }
        };
        method.invoke(client, callback);

        countDownLatch.await();
        return result.get();
      } catch (InvocationTargetException e) {
        /*if socket failed create a new socket. */

        if (e.getTargetException() instanceof DisconnectionException) {
          serviceClients.set(0, generateClient());
          logger.error(
              "the service fails to call the method ping(), " 
                  + "and now create a socket again, endpoint={}",
              appContext.getMainEndPoint());
          return false;

        } else {
          logger.error("the service fails to call the method ping()", e.getTargetException());
          throw e;
        }
      }
    } catch (IllegalArgumentException | IllegalAccessException e) {
      logger.error("the service doesn't have ping() method defined", e);
      throw e;
    } catch (Exception e) {
      logger.info("Unknown exception", e);
      throw e;
    } finally {
      logger.info("nothing need to do here");
    }
  }

  @Override
  protected void initHeartbeatItselfFactory() throws Exception {
    Map<PortType, EndPoint> endPoints = appContext.getEndPoints();
    if (healthCheckerClientFactory == null || endPoints.size() == 0) {
      throw new Exception("ServiceClientClass or service port are not set yet");
    }

    Object serviceClient = generateClient();

    validateServiceHealthCheckMethodExists(serviceClient);
    Method method = serviceClient.getClass()
        .getMethod(SERVICE_HEALTH_CHECK_METHOD_NAME, MethodCallback.class);

    methods.add(method);
    serviceClients.add(serviceClient);
  }

  protected void validateServiceHealthCheckMethodExists(Object clientObject) throws Exception {
    try {
      clientObject.getClass().getMethod(SERVICE_HEALTH_CHECK_METHOD_NAME, MethodCallback.class);
    } catch (SecurityException | NoSuchMethodException e) {
      logger.error("the service doesn't have heath check method defined", e);
      throw e;
    } catch (Exception e) {
      logger.error("something wrong with the client object", e);
      throw e;
    }
  }

  private Object generateClient() {
    return healthCheckerClientFactory
        .generateSyncClient(appContext.getMainEndPoint());
  }
}
