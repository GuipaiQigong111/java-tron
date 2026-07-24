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
import static org.junit.Assert.fail;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.netty.GrpcHttp2ConnectionHandler;
import io.grpc.netty.InternalProtocolNegotiator;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import org.junit.Test;

public class GrpcNettyMaxConcurrentStreamsLimiterTest {

  @Test
  public void shouldEnforceMaxConcurrentStreamsBeforeInstallingGrpcHandler() throws Exception {
    InternalProtocolNegotiator.ProtocolNegotiator delegate =
        mock(InternalProtocolNegotiator.ProtocolNegotiator.class);
    GrpcHttp2ConnectionHandler grpcHandler = mock(GrpcHttp2ConnectionHandler.class);
    ChannelHandler channelHandler = mock(ChannelHandler.class);
    Http2Connection connection = new DefaultHttp2Connection(true);
    when(grpcHandler.connection()).thenReturn(connection);
    when(delegate.newHandler(grpcHandler)).thenReturn(channelHandler);

    GrpcNettyMaxConcurrentStreamsLimiter.EnforcingProtocolNegotiator negotiator =
        new GrpcNettyMaxConcurrentStreamsLimiter.EnforcingProtocolNegotiator(delegate, 2);

    assertSame(channelHandler, negotiator.newHandler(grpcHandler));
    assertEquals(2, connection.remote().maxActiveStreams());
    verify(delegate).newHandler(grpcHandler);

    connection.remote().createStream(1, false);
    connection.remote().createStream(3, false);
    try {
      connection.remote().createStream(5, false);
      fail("Third concurrent stream should be refused");
    } catch (Http2Exception e) {
      assertEquals(Http2Error.REFUSED_STREAM, e.error());
    }
  }
}
