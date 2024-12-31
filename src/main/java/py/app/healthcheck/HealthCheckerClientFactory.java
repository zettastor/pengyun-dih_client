

package py.app.healthcheck;

import py.common.struct.EndPoint;

public interface HealthCheckerClientFactory<T> {
  T generateSyncClient(final EndPoint endPoint);
}
