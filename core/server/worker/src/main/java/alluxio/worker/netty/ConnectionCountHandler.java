/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.netty;

import alluxio.worker.block.BlockWorker;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The Netty server handler to count the number of active connections.
 */
@ThreadSafe
public final class ConnectionCountHandler extends ChannelInboundHandlerAdapter {
  private final BlockWorker mBlockWorker;

  /**
   * @param blockWorker the block worker
   */
  public ConnectionCountHandler(BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) {
    if (Utils.isCapabilityEnabled()) {
      mBlockWorker.getCapabilityCache().incrementUserConnectionCount(
          ctx.channel().attr(alluxio.netty.NettyAttributes.CHANNEL_KERBEROS_USER_KEY).get());
    }
    ctx.fireChannelRegistered();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    if (Utils.isCapabilityEnabled()) {
      String user =
          ctx.channel().attr(alluxio.netty.NettyAttributes.CHANNEL_KERBEROS_USER_KEY).get();
      if (user != null) {
        mBlockWorker.getCapabilityCache().decrementUserConnectionCount(user);
      }
    }
    ctx.fireChannelUnregistered();
  }
}
