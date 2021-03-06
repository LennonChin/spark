/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Wraps another BlockFetcher with the ability to automatically retry fetches which fail due to
 * IOExceptions, which we hope are due to transient network conditions.
 *
 * This fetcher provides stronger guarantees regarding the parent BlockFetchingListener. In
 * particular, the listener will be invoked exactly once per blockId, with a success or failure.
 */
public class RetryingBlockFetcher {

  /**
   * Used to initiate the first fetch for all blocks, and subsequently for retrying the fetch on any
   * remaining blocks.
   */
  public interface BlockFetchStarter {
    /**
     * Creates a new BlockFetcher to fetch the given block ids which may do some synchronous
     * bootstrapping followed by fully asynchronous block fetching.
     * The BlockFetcher must eventually invoke the Listener on every input blockId, or else this
     * method must throw an exception.
     *
     * This method should always attempt to get a new TransportClient from the
     * {@link org.apache.spark.network.client.TransportClientFactory} in order to fix connection
     * issues.
     */
    void createAndStart(String[] blockIds, BlockFetchingListener listener) throws IOException;
  }

  /** Shared executor service used for waiting and retrying.
   * 用于重试的线程池
   **/
  private static final ExecutorService executorService = Executors.newCachedThreadPool(
    NettyUtils.createThreadFactory("Block Fetch Retry"));

  private static final Logger logger = LoggerFactory.getLogger(RetryingBlockFetcher.class);

  /** Used to initiate new Block Fetches on our remaining blocks.
   * 拉取启动器
   **/
  private final BlockFetchStarter fetchStarter;

  /** Parent listener which we delegate all successful or permanently failed block fetches to.
   * 用于记录外界传入的监听器
   **/
  private final BlockFetchingListener listener;

  /** Max number of times we are allowed to retry.
   * 最大重试次数
   **/
  private final int maxRetries;

  /** Milliseconds to wait before each retry.
   * 两次重试之间的时间间隔，由spark.模块名.io.retryWait决定
   **/
  private final int retryWaitTime;

  // NOTE:
  // All of our non-final fields are synchronized under 'this' and should only be accessed/mutated
  // while inside a synchronized block.
  /** Number of times we've attempted to retry so far.
   * 记录已经尝试拉取的次数
   **/
  private int retryCount = 0;

  /**
   * Set of all block ids which have not been fetched successfully or with a non-IO Exception.
   * A retry involves requesting every outstanding block. Note that since this is a LinkedHashSet,
   * input ordering is preserved, so we always request blocks in the same order the user provided.
   *
   * 记录需要拉取的数据块的BlockId集合
   */
  private final LinkedHashSet<String> outstandingBlocksIds;

  /**
   * The BlockFetchingListener that is active with our current BlockFetcher.
   * When we start a retry, we immediately replace this with a new Listener, which causes all any
   * old Listeners to ignore all further responses.
   *
   * 重试监听器
   */
  private RetryingBlockFetchListener currentListener;

  public RetryingBlockFetcher(
      TransportConf conf,
      BlockFetchStarter fetchStarter,
      String[] blockIds,
      BlockFetchingListener listener) {
    // 记录拉取启动器
    this.fetchStarter = fetchStarter;
    // 记录传入的BlockFetchingListener监听器
    this.listener = listener;
    // 获取配置的最大重试次数
    this.maxRetries = conf.maxIORetries();
    // 获取配置两次重试的间隔等待时间
    this.retryWaitTime = conf.ioRetryWaitTimeMs();
    // 将需要拉取的数据块的BlockId全部放入outstandingBlocksIds保存
    this.outstandingBlocksIds = Sets.newLinkedHashSet();
    Collections.addAll(outstandingBlocksIds, blockIds);
    // 创建新的监听器
    this.currentListener = new RetryingBlockFetchListener();
  }

  /**
   * Initiates the fetch of all blocks provided in the constructor, with possible retries in the
   * event of transient IOExceptions.
   */
  public void start() {
    // 拉取所有的待拉取数据块
    fetchAllOutstanding();
  }

  /**
   * Fires off a request to fetch all blocks that have not been fetched successfully or permanently
   * failed (i.e., by a non-IOException).
   */
  private void fetchAllOutstanding() {
    // Start by retrieving our shared state within a synchronized block.
    // 还需要被拉取的数据块的BlockId
    String[] blockIdsToFetch;
    // 重试次数
    int numRetries;
    RetryingBlockFetchListener myListener;
    synchronized (this) {
      // 记录还需要拉取的数据块的BlockId
      blockIdsToFetch = outstandingBlocksIds.toArray(new String[outstandingBlocksIds.size()]);
      // 记录重试次数
      numRetries = retryCount;
      // 记录监听器
      myListener = currentListener;
    }

    // Now initiate the fetch on all outstanding blocks, possibly initiating a retry if that fails.
    try {
      // 这里调用的是RetryingBlockFetcher.BlockFetchStarter对象的createAndStart()方法，会返回OneForOneBlockFetcher对象
      fetchStarter.createAndStart(blockIdsToFetch, myListener);
    } catch (Exception e) {
      logger.error(String.format("Exception while beginning fetch of %s outstanding blocks %s",
        blockIdsToFetch.length, numRetries > 0 ? "(after " + numRetries + " retries)" : ""), e);

      // 发生异常，判断是否还可以重试
      if (shouldRetry(e)) { // 还可以重试
        // 再次重试，此处会向线程池提交一个新的任务执行fetchAllOutstanding()方法
        initiateRetry();
      } else {
        /**
         * 没有重试次数了，通知listener产生的异常
         * 注意，这里的bid是还没有拉取的数据块的BlockId
         */
        for (String bid : blockIdsToFetch) {
          listener.onBlockFetchFailure(bid, e);
        }
      }
    }
  }

  /**
   * Lightweight method which initiates a retry in a different thread. The retry will involve
   * calling fetchAllOutstanding() after a configured wait time.
   *
   * 开启一次新的重试
   */
  private synchronized void initiateRetry() {
    // 重试次数自增
    retryCount += 1;
    // 创建RetryingBlockFetchListener监听器
    currentListener = new RetryingBlockFetchListener();

    logger.info("Retrying fetch ({}/{}) for {} outstanding blocks after {} ms",
      retryCount, maxRetries, outstandingBlocksIds.size(), retryWaitTime);

    // 向线程池提交一个任务，任务内容是执行fetchAllOutstanding()方法
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
        // 调用fetchAllOutstanding()方法再次尝试拉取
        fetchAllOutstanding();
      }
    });
  }

  /**
   * Returns true if we should retry due a block fetch failure. We will retry if and only if
   * the exception was an IOException and we haven't retried 'maxRetries' times already.
   */
  private synchronized boolean shouldRetry(Throwable e) {
    // IOException才重试
    boolean isIOException = e instanceof IOException
      || (e.getCause() != null && e.getCause() instanceof IOException);
    // 还有剩余重试次数
    boolean hasRemainingRetries = retryCount < maxRetries;
    return isIOException && hasRemainingRetries;
  }

  /**
   * Our RetryListener intercepts block fetch responses and forwards them to our parent listener.
   * Note that in the event of a retry, we will immediately replace the 'currentListener' field,
   * indicating that any responses from non-current Listeners should be ignored.
   */
  private class RetryingBlockFetchListener implements BlockFetchingListener {

    // 拉取数据成功的回调
    @Override
    public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
      // We will only forward this success message to our parent listener if this block request is
      // outstanding and we are still the active listener.
      // 用于指定是否需要将成功获取的数据转发给listener
      boolean shouldForwardSuccess = false;
      synchronized (RetryingBlockFetcher.this) {
        /**
         * 1. 判断监听器是否被改变，每次重新拉取都会重置currentListener为新的RetryingBlockFetchListener对象；
         *    因此如果currentListener与当前RetryingBlockFetchListener对象不一致，说明这次的拉取已经过期了。
         * 2. 拉取的数据块是否是需要的；outstandingBlocksIds会在每次成功拉取后将当次拉取的数据库的BlockId移除，
         *    防止重复拉取；如果拉取的数据块的BlockId不被outstandingBlocksIds包含，说明重复拉取了。
         */
        if (this == currentListener && outstandingBlocksIds.contains(blockId)) {
          // 满足条件，先把本次拉取的数据块的BlockId记录从outstandingBlocksIds移除
          outstandingBlocksIds.remove(blockId);
          // 标记本次拉取的数据可以转发给listener
          shouldForwardSuccess = true;
        }
      }

      // Now actually invoke the parent listener, outside of the synchronized block.
      if (shouldForwardSuccess) {
        // 将拉取的数据转发给listener
        listener.onBlockFetchSuccess(blockId, data);
      }
    }

    @Override
    public void onBlockFetchFailure(String blockId, Throwable exception) {
      // We will only forward this failure to our parent listener if this block request is
      // outstanding, we are still the active listener, AND we cannot retry the fetch.
      // 标记是否需要转发异常
      boolean shouldForwardFailure = false;
      synchronized (RetryingBlockFetcher.this) {
        // 该判断与上面的onBlockFetchSuccess()方法中的判断是一致的
        if (this == currentListener && outstandingBlocksIds.contains(blockId)) {
          // 判断是否还需要重试，只有在IOException和还有剩余重试次数时才重试
          if (shouldRetry(exception)) {
            // 准备重试
            initiateRetry();
          } else {
            logger.error(String.format("Failed to fetch block %s, and will not retry (%s retries)",
              blockId, retryCount), exception);
            // 没有重试次数或者发生了其他异常，将本次拉取的数据块的BlockId记录从outstandingBlocksIds移除
            outstandingBlocksIds.remove(blockId);
            // 标记本次产生的异常可以转发给listener
            shouldForwardFailure = true;
          }
        }
      }

      // Now actually invoke the parent listener, outside of the synchronized block.
      if (shouldForwardFailure) {
        // 将产生的异常转发给listener
        listener.onBlockFetchFailure(blockId, exception);
      }
    }
  }
}
