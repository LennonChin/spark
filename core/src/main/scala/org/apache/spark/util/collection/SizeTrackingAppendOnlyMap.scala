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

package org.apache.spark.util.collection

/**
 * An append-only map that keeps track of its estimated size in bytes.
 *
 * 该类是SizeTracker和AppendOnlyMap的结合体，可以在内存中对任务执行结果进行更新或聚合运算，
 * 也可以对自身的大小进行样本采集和大小估算
 */
private[spark] class SizeTrackingAppendOnlyMap[K, V]
  extends AppendOnlyMap[K, V] with SizeTracker {

  // 更新或添加键值对
  override def update(key: K, value: V): Unit = {
    // 使用AppendOnlyMap的update()方法更新
    super.update(key, value)
    // 调用SizeTracker的方法完成采样
    super.afterUpdate()
  }

  // 聚合操作
  override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    // 使用AppendOnlyMap的changeValue()方法进行聚合
    val newValue = super.changeValue(key, updateFunc)
    // 调用SizeTracker的方法完成采样
    super.afterUpdate()
    // 返回聚合得到的新的值
    newValue
  }

  // 扩容操作
  override protected def growTable(): Unit = {
    // 调用AppendOnlyMap的growTable()方法进行聚合
    super.growTable()
    // 调用SizeTracker的方法对样本进行重置，提高AppendOnlyMap的大小估算准确性
    resetSamples()
  }
}
