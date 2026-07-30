/*
 * java-tron is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * java-tron is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with java-tron.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.tron.common.application;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class GrpcConnectionLimiterTest {

  @Test
  public void shouldEnforceGlobalLimitAcrossRemoteIps() throws Exception {
    GrpcConnectionLimiter limiter = new GrpcConnectionLimiter();
    InetAddress firstIp = InetAddress.getByName("192.0.2.1");
    InetAddress secondIp = InetAddress.getByName("192.0.2.2");
    InetAddress thirdIp = InetAddress.getByName("192.0.2.3");

    GrpcConnectionLimiter.Permit first = limiter.tryAcquire(
        new InetSocketAddress(firstIp, 10001), 2, 2);
    GrpcConnectionLimiter.Permit second = limiter.tryAcquire(
        new InetSocketAddress(secondIp, 10002), 2, 2);

    assertNotNull(first);
    assertNotNull(second);
    assertNull(limiter.tryAcquire(new InetSocketAddress(thirdIp, 10003), 2, 2));
    assertEquals(2, limiter.activeConnections());

    first.release();
    second.release();
    assertEquals(0, limiter.activeConnections());
  }

  @Test
  public void shouldEnforcePerIpLimitAndAllowOtherIps() throws Exception {
    GrpcConnectionLimiter limiter = new GrpcConnectionLimiter();
    InetAddress firstIp = InetAddress.getByName("192.0.2.10");
    InetAddress secondIp = InetAddress.getByName("192.0.2.11");

    GrpcConnectionLimiter.Permit first = limiter.tryAcquire(
        new InetSocketAddress(firstIp, 10001), 3, 1);

    assertNotNull(first);
    assertNull(limiter.tryAcquire(new InetSocketAddress(firstIp, 10002), 3, 1));
    GrpcConnectionLimiter.Permit second = limiter.tryAcquire(
        new InetSocketAddress(secondIp, 10003), 3, 1);
    assertNotNull(second);
    assertEquals(1, limiter.activeConnections(firstIp));
    assertEquals(1, limiter.activeConnections(secondIp));

    first.release();
    second.release();
    assertEquals(0, limiter.activeConnections(firstIp));
    assertEquals(0, limiter.activeConnections(secondIp));
  }

  @Test
  public void shouldReleasePermitOnlyOnceAndAllowReplacementConnection() throws Exception {
    GrpcConnectionLimiter limiter = new GrpcConnectionLimiter();
    InetAddress remoteIp = InetAddress.getByName("192.0.2.20");
    InetSocketAddress remoteAddress = new InetSocketAddress(remoteIp, 10001);
    GrpcConnectionLimiter.Permit first = limiter.tryAcquire(remoteAddress, 1, 1);

    assertNotNull(first);
    first.release();
    first.release();
    assertEquals(0, limiter.activeConnections());

    GrpcConnectionLimiter.Permit replacement = limiter.tryAcquire(remoteAddress, 1, 1);
    assertNotNull(replacement);
    replacement.release();
    assertEquals(0, limiter.activeConnections());
  }

  @Test
  public void shouldNotExceedLimitDuringConcurrentAdmissions() throws Exception {
    int maxConnections = 8;
    int attempts = 64;
    GrpcConnectionLimiter limiter = new GrpcConnectionLimiter();
    InetAddress remoteIp = InetAddress.getByName("192.0.2.30");
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch attempted = new CountDownLatch(attempts);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger admitted = new AtomicInteger();
    ExecutorService executor = Executors.newFixedThreadPool(32);
    List<Future<?>> futures = new ArrayList<>();

    try {
      for (int i = 0; i < attempts; i++) {
        int port = 10000 + i;
        futures.add(executor.submit(() -> {
          start.await();
          GrpcConnectionLimiter.Permit permit = limiter.tryAcquire(
              new InetSocketAddress(remoteIp, port), maxConnections, maxConnections);
          if (permit != null) {
            admitted.incrementAndGet();
          }
          attempted.countDown();
          if (permit != null) {
            release.await();
            permit.release();
          }
          return null;
        }));
      }

      start.countDown();
      assertTrue(attempted.await(5, TimeUnit.SECONDS));
      assertEquals(maxConnections, admitted.get());
      assertEquals(maxConnections, limiter.activeConnections());
      assertEquals(maxConnections, limiter.activeConnections(remoteIp));
      release.countDown();
      for (Future<?> future : futures) {
        future.get(5, TimeUnit.SECONDS);
      }
      assertEquals(0, limiter.activeConnections());
      assertEquals(0, limiter.activeConnections(remoteIp));
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }
}
