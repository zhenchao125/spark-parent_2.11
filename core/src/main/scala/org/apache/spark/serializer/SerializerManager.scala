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

package org.apache.spark.serializer

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.storage._
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

/**
 * Component which configures serialization, compression and encryption for various Spark
 * components, including automatic selection of which [[Serializer]] to use for shuffles.
 */
private[spark] class SerializerManager(
    defaultSerializer: Serializer,
    conf: SparkConf,
    // 加密使用的秘钥
    encryptionKey: Option[Array[Byte]]) {

  def this(defaultSerializer: Serializer, conf: SparkConf) = this(defaultSerializer, conf, None)
  // spark的提供的另外一种序列化机制
  private[this] val kryoSerializer = new KryoSerializer(conf)

  private[this] val stringClassTag: ClassTag[String] = implicitly[ClassTag[String]]
  private[this] val primitiveAndPrimitiveArrayClassTags: Set[ClassTag[_]] = {
    val primitiveClassTags = Set[ClassTag[_]](
      ClassTag.Boolean,
      ClassTag.Byte,
      ClassTag.Char,
      ClassTag.Double,
      ClassTag.Float,
      ClassTag.Int,
      ClassTag.Long,
      ClassTag.Null,
      ClassTag.Short
    )
    val arrayClassTags = primitiveClassTags.map(_.wrap)
    primitiveClassTags ++ arrayClassTags
  }

  // Whether to compress broadcast variables that are stored
  // 是否对广播变量进行压缩
  private[this] val compressBroadcast = conf.getBoolean("spark.broadcast.compress", true)
  // Whether to compress shuffle output that are stored
  // 是否对shuffle的输出进行压缩
  private[this] val compressShuffle = conf.getBoolean("spark.shuffle.compress", true)
  // Whether to compress RDD partitions that are stored serialized
  // 是否对RDD进行压缩
  private[this] val compressRdds = conf.getBoolean("spark.rdd.compress", false)
  // Whether to compress shuffle output temporarily spilled to disk
  // 是否对溢出到磁盘的数据进行压缩
  private[this] val compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true)

  /* The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet. */
  // 压缩编码
  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)
  // 是否支持加密
  def encryptionEnabled: Boolean = encryptionKey.isDefined
  // 对于制定的类型ct是否使用 Cryo 序列化器
  def canUseKryo(ct: ClassTag[_]): Boolean = {
    primitiveAndPrimitiveArrayClassTags.contains(ct) || ct == stringClassTag
  }

  // SPARK-18617: As feature in SPARK-13990 can not be applied to Spark Streaming now. The worst
  // result is streaming job based on `Receiver` mode can not run on Spark 2.x properly. It may be
  // a rational choice to close `kryo auto pick` feature for streaming in the first step.
  def getSerializer(ct: ClassTag[_], autoPick: Boolean): Serializer = {
    if (autoPick && canUseKryo(ct)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  /**
   * Pick the best serializer for shuffling an RDD of key-value pairs.
   */
  def getSerializer(keyClassTag: ClassTag[_], valueClassTag: ClassTag[_]): Serializer = {
    if (canUseKryo(keyClassTag) && canUseKryo(valueClassTag)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId => compressShuffle
      case _: BroadcastBlockId => compressBroadcast
      case _: RDDBlockId => compressRdds
      case _: TempLocalBlockId => compressShuffleSpill
      case _: TempShuffleBlockId => compressShuffle
      case _ => false
    }
  }

  /**
   * Wrap an input stream for encryption and compression
   */
  def wrapStream(blockId: BlockId, s: InputStream): InputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
   * Wrap an output stream for encryption and compression
   */
  def wrapStream(blockId: BlockId, s: OutputStream): OutputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
   * Wrap an input stream for encryption if shuffle encryption is enabled
   */
  def wrapForEncryption(s: InputStream): InputStream = {
    encryptionKey
      .map { key => CryptoStreamUtils.createCryptoInputStream(s, conf, key) }
      .getOrElse(s)
  }

  /**
   * Wrap an output stream for encryption if shuffle encryption is enabled
   */
  def wrapForEncryption(s: OutputStream): OutputStream = {
    encryptionKey
      .map { key => CryptoStreamUtils.createCryptoOutputStream(s, conf, key) }
      .getOrElse(s)
  }

  /**
   * Wrap an output stream for compression if block compression is enabled for its block type
   */
  private[this] def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
   */
  private[this] def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
  }

  /** Serializes into a stream. */
  def dataSerializeStream[T: ClassTag](
      blockId: BlockId,
      outputStream: OutputStream,
      values: Iterator[T]): Unit = {
    val byteStream = new BufferedOutputStream(outputStream)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = getSerializer(implicitly[ClassTag[T]], autoPick).newInstance()
    ser.serializeStream(wrapStream(blockId, byteStream)).writeAll(values).close()
  }

  /** Serializes into a chunked byte buffer. */
  def dataSerialize[T: ClassTag](
      blockId: BlockId,
      values: Iterator[T],
      allowEncryption: Boolean = true): ChunkedByteBuffer = {
    dataSerializeWithExplicitClassTag(blockId, values, implicitly[ClassTag[T]],
      allowEncryption = allowEncryption)
  }

  /** Serializes into a chunked byte buffer. */
  def dataSerializeWithExplicitClassTag(
      blockId: BlockId,
      values: Iterator[_],
      classTag: ClassTag[_],
      allowEncryption: Boolean = true): ChunkedByteBuffer = {
    val bbos = new ChunkedByteBufferOutputStream(1024 * 1024 * 4, ByteBuffer.allocate)
    val byteStream = new BufferedOutputStream(bbos)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val ser = getSerializer(classTag, autoPick).newInstance()
    val encrypted = if (allowEncryption) wrapForEncryption(byteStream) else byteStream
    ser.serializeStream(wrapForCompression(blockId, encrypted)).writeAll(values).close()
    bbos.toChunkedByteBuffer
  }

  /**
   * Deserializes an InputStream into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserializeStream[T](
      blockId: BlockId,
      inputStream: InputStream,
      maybeEncrypted: Boolean = true)
      (classTag: ClassTag[T]): Iterator[T] = {
    val stream = new BufferedInputStream(inputStream)
    val autoPick = !blockId.isInstanceOf[StreamBlockId]
    val decrypted = if (maybeEncrypted) wrapForEncryption(inputStream) else inputStream
    getSerializer(classTag, autoPick)
      .newInstance()
      .deserializeStream(wrapForCompression(blockId, decrypted))
      .asIterator.asInstanceOf[Iterator[T]]
  }
}
