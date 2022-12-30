package py.aop.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used to specify the information of the method which is extends from the thrift
 * client as follows: 1.whether the method needs to retry. 2.retry times & wait period. 3.the
 * condition of retry.
 *
 * @author sxl
 * @usage:
 *         1: if you want to retry 4 times when there is an
 *         InternalError_Thrift exception threw out on executing method
 *         ControlCenterImpl.createVolume,what you need to do is add a retry
 *         annotation
 *         just like this:
 *
 *         {@code @Retry}(times = 4, period = 1,when=InternalError_Thrift.class)
 *         ...(function in the class who is inherit from thrift client)
 *
 *
 *         2.if you have not give the retry parameter, the 'retry framework'
 *         will retry three times on all exceptions, and between each retry
 *         execution, the thread will sleep 1 second.the usage is like this:
 *
 *         {@code @Retry}<br>
 *         ...(function in the class who is inherit from thrift client)
 *
 *         3.if you want to do retry on more than 1 exceptions, you can do it
 *         like this: <br>
 *         {@code @Retry}
 *         (when={InternalError_Thrift.class,AccessDeniedException_Thrift)
 *         ...(function in the class who is inherit from thrift client)
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Retry {
  /**
   * time.
   *
   * @return int
   */
  public int times() default 3;

  /**
   * period.
   *
   * @return int
   */
  public int period() default 1;

  /**
   * when.
   *
   * @return Throwable
   */
  public Class<? extends Throwable>[] when() default None.class;

  /**
   * Default empty exception.
   */
  static class None extends Throwable {
    private static final long serialVersionUID = 1L;

    private None() {
    }
  }
}
