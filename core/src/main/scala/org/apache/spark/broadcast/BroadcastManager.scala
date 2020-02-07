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

package org.apache.spark.broadcast

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.internal.Logging
import org.apache.spark.{SecurityManager, SparkConf}

import scala.reflect.ClassTag

private[spark] class BroadcastManager(
                                         val isDriver: Boolean,
                                         conf: SparkConf,
                                         securityManager: SecurityManager)
    extends Logging {
    //表示广播管理器是否初始化完成的状态
    private var initialized = false
    // 广播工程实例
    private var broadcastFactory: BroadcastFactory = null
    
    initialize()
    
    // Called by SparkContext or Executor before using Broadcast
    // 初始化广播管理器
    private def initialize() {
        synchronized {
            if (!initialized) { // 保证广播管理器只被初始化一次
                // 创建广播工程实例
                broadcastFactory = new TorrentBroadcastFactory
                // 对广播工厂进程初始化
                broadcastFactory.initialize(isDriver, conf, securityManager)
                // 把初始化标记设置为true
                initialized = true
            }
        }
    }
    
    def stop() {
        broadcastFactory.stop()
    }
    // 下一个广播对象的id
    private val nextBroadcastId = new AtomicLong(0)
    
    def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
        broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
    }
    
    def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
        broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
    }
}
