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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Limits active incoming gRPC connections across all gRPC server ports. */
final class GrpcConnectionLimiter {

  private final AtomicInteger activeConnections = new AtomicInteger();
  private final ConcurrentMap<InetAddress, Integer> activeConnectionsByIp =
      new ConcurrentHashMap<>();

  ChannelHandler newHandler(
      ChannelHandler delegate, int maxConnections, int maxConnectionsPerIp) {
    checkNotNull(delegate, "delegate");
    checkArgument(maxConnections > 0, "maxConnections must be positive");
    checkArgument(maxConnectionsPerIp > 0, "maxConnectionsPerIp must be positive");
    checkArgument(maxConnectionsPerIp <= maxConnections,
        "maxConnectionsPerIp must not exceed maxConnections");
    return new ConnectionAdmissionHandler(
        delegate, this, maxConnections, maxConnectionsPerIp);
  }

  Permit tryAcquire(
      SocketAddress remoteAddress, int maxConnections, int maxConnectionsPerIp) {
    if (!tryIncrement(activeConnections, maxConnections)) {
      return null;
    }

    InetAddress remoteIp = remoteIp(remoteAddress);
    if (remoteIp != null && !tryIncrement(remoteIp, maxConnectionsPerIp)) {
      activeConnections.decrementAndGet();
      return null;
    }
    return new Permit(this, remoteIp);
  }

  int activeConnections() {
    return activeConnections.get();
  }

  int activeConnections(InetAddress remoteIp) {
    return activeConnectionsByIp.getOrDefault(remoteIp, 0);
  }

  private boolean tryIncrement(InetAddress remoteIp, int maxConnectionsPerIp) {
    AtomicBoolean acquired = new AtomicBoolean();
    activeConnectionsByIp.compute(remoteIp, (ignored, current) -> {
      int count = current == null ? 0 : current;
      if (count >= maxConnectionsPerIp) {
        return current;
      }
      acquired.set(true);
      return count + 1;
    });
    return acquired.get();
  }

  private static boolean tryIncrement(AtomicInteger counter, int limit) {
    while (true) {
      int current = counter.get();
      if (current >= limit) {
        return false;
      }
      if (counter.compareAndSet(current, current + 1)) {
        return true;
      }
    }
  }

  private void release(InetAddress remoteIp) {
    if (remoteIp != null) {
      activeConnectionsByIp.compute(remoteIp, (ignored, current) -> {
        if (current == null || current <= 1) {
          return null;
        }
        return current - 1;
      });
    }
    activeConnections.decrementAndGet();
  }

  private static InetAddress remoteIp(SocketAddress remoteAddress) {
    if (!(remoteAddress instanceof InetSocketAddress)) {
      return null;
    }
    return ((InetSocketAddress) remoteAddress).getAddress();
  }

  static final class Permit {

    private final GrpcConnectionLimiter limiter;
    private final InetAddress remoteIp;
    private final AtomicBoolean released = new AtomicBoolean();

    private Permit(GrpcConnectionLimiter limiter, InetAddress remoteIp) {
      this.limiter = limiter;
      this.remoteIp = remoteIp;
    }

    void release() {
      if (released.compareAndSet(false, true)) {
        limiter.release(remoteIp);
      }
    }
  }

  private static final class ConnectionAdmissionHandler extends ChannelInboundHandlerAdapter {

    private final ChannelHandler delegate;
    private final GrpcConnectionLimiter limiter;
    private final int maxConnections;
    private final int maxConnectionsPerIp;

    private ConnectionAdmissionHandler(
        ChannelHandler delegate,
        GrpcConnectionLimiter limiter,
        int maxConnections,
        int maxConnectionsPerIp) {
      this.delegate = delegate;
      this.limiter = limiter;
      this.maxConnections = maxConnections;
      this.maxConnectionsPerIp = maxConnectionsPerIp;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      Permit permit = limiter.tryAcquire(
          ctx.channel().remoteAddress(), maxConnections, maxConnectionsPerIp);
      if (permit == null) {
        ctx.close();
        return;
      }

      ctx.channel().closeFuture().addListener(ignored -> permit.release());
      ctx.pipeline().addAfter(ctx.name(), null, delegate);
    }
  }
}
