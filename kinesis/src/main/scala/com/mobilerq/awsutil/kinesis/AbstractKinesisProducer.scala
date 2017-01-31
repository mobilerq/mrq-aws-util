package com.mobilerq.awsutil.kinesis

import java.nio.ByteBuffer
import javax.annotation.{PostConstruct, PreDestroy}

import com.amazonaws.services.kinesis.producer.{KinesisProducer, KinesisProducerConfiguration, UserRecordFailedException, UserRecordResult}
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.util.concurrent.{FutureCallback, Futures}
import org.apache.commons.logging.LogFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

/**
  * Write records of type T to kinesis as JSON using jackson [[ObjectMapper]]
  *
  * @tparam T the type
  */
trait AbstractKinesisProducer[T <: AnyRef] {
  private val log = LogFactory.getLog(getClass)

  /**
    * The kinesis stream name
    */
  val streamName: String
  /**
    * The kinesis region
    */
  val region: String
  /**
    * [[KinesisProducerConfiguration]] Record Max Buffered Time
    */
  val maxBufferMs: Long
  /**
    * [[KinesisProducerConfiguration]] Max Connections
    */
  val maxConnections: Long

  /**
    * Generate configuration generated from  properties. The config is passed to [[KinesisProducer]] during [[start()]].
    *
    * @return A KinesisProducerConfiguration
    */
  def kinesisProducerConfiguration = new KinesisProducerConfiguration()
    .setRegion(region)
    .setRecordMaxBufferedTime(maxBufferMs)
    .setMaxConnections(maxConnections)

  /**
    * The jackson mapper
    */
  val mapper = new ObjectMapper

  /**
    * The kinesis producer created during [[AbstractKinesisProducer#start()]]
    */
  var kinesisProducer: KinesisProducer = _

  /**
    * Send a record into the record stream by calling [[KinesisProducer]] addUserRecord.
    * The record will be serialized to JSON bytes using the [[ObjectMapper]].
    * Errors are sent to [[onError(t)]].
    *
    * @param record The record to send
    * @return a Future of [[UserRecordResult]]
    */
  def put(record: T): Future[UserRecordResult] = try {
    val p = Promise[UserRecordResult]()
    val bytes = ByteBuffer.wrap(mapper.writeValueAsBytes(record))
    val future = kinesisProducer.addUserRecord(streamName, partitionKey(record), bytes)
    Futures.addCallback(future, new FutureCallback[UserRecordResult] {
      override def onSuccess(result: UserRecordResult) = p.success(result)

      override def onFailure(ex: Throwable) = {
        onError(ex)
        p.failure(ex)
      }
    })
    p.future
  } catch {
    case NonFatal(ex) ⇒
      onError(ex)
      Future.failed(ex)
  }

  /**
    * An alias for put just because it looks awesome
    */
  def apply(record: T): Unit = put(record)

  /**
    * Implement this method to map to the partition key to be used for record t.
    */
  def partitionKey(record: T): String

  /**
    * Create and start the producer
    */
  @PostConstruct
  def start(): Unit = synchronized {
    if (kinesisProducer == null) {
      kinesisProducer = new KinesisProducer(kinesisProducerConfiguration)
    }
  }

  /**
    * Shutdown the producer by flushing child process buffers and destroying child processes.
    */
  @PreDestroy
  def shutdown(): Unit = synchronized {
    if (kinesisProducer != null) {
      try {
        kinesisProducer.flushSync()
        kinesisProducer.destroy()
      } catch {
        case NonFatal(ex) ⇒ onError(ex)
      }
      kinesisProducer = null
    }
  }


  /**
    * Called when any error occurs. Default logs the error.
    *
    * @param t the error
    */
  def onError(t: Throwable) = {
    val message = t match {
      case e: UserRecordFailedException ⇒
        val result = e.getResult
        s"Kinesis Producer (stream=$streamName) " +
          s"UserRecordResult successful=${result.isSuccessful} attempts=${result.getAttempts.size} " +
          result.getAttempts.asScala.map { a ⇒
            s"Attempt delay=${a.getDelay} duration=${a.getDuration} errorCode=${a.getErrorCode} errorMessage=${a.getErrorMessage}"
          }.mkString(", ")
      case _ ⇒
        s"Kinesis Producer (stream=$streamName)"
    }
    log.error(message, t)
  }
}
