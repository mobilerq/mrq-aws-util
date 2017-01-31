package com.mobilerq.awsutil.kinesis

import java.net.InetAddress
import java.util.UUID._
import java.util.concurrent.Executors
import javax.annotation.{PostConstruct, PreDestroy}

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, ShutdownReason, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types._
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.logging.LogFactory

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * A simple kinesis consumer that maps incoming records to records of type T. Defaults using a jackson [[ObjectMapper]].
  *
  * @tparam T The target type
  */
trait AbstractKinesisConsumer[T <: AnyRef] extends IRecordProcessorFactory {

  /**
    * The target class type
    */
  val classObject: Class[T]
  /**
    * The kinesis stream name
    */
  val streamName: String
  /**
    * The kinesis region
    */
  val region: String
  /**
    * The kinesis application
    */
  val applicationName: String

  private val log = LogFactory.getLog(getClass)
  private var worker: Worker = _
  implicit private val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)

  /**
    * Implement this method to provide processing logic. Optionally call checkpoint.
    * Any exceptions are logged and sent to New Relic.
    * Each distinct applicationName defines a unique "subscription to a topic" and is an independent consumer
    * from the stream. Remember Kinesis has "at least once semantics", so any downstream consumers will need
    * to store state in order to prevent duplicate processing.
    */
  def process(t: List[T], checkpoint: () ⇒ Unit): Unit

  /**
    * Optionally override this method to define shard termination behavior.
    */
  def onTerminate(checkpoint: () ⇒ Unit): Unit = checkpoint()

  /**
    * Log errors using slf4j.
    *
    * @param t The error
    */
  def onError(t: Throwable) = {
    log.error(s"Kinesis Client Library (stream=$streamName application=$applicationName)", t)
  }

  /**
    * Defines the Kinesis configuration used by this consumer.
    * Subclass may override this method to provide specialized configuration.
    */
  def kinesisClientLibConfiguration: KinesisClientLibConfiguration = {
    val credentialsProviderChain = new DefaultAWSCredentialsProviderChain
    val workerId = InetAddress.getLocalHost.getCanonicalHostName + ":" + randomUUID()
    new KinesisClientLibConfiguration(
      applicationName,
      streamName,
      credentialsProviderChain,
      workerId)
      .withRegionName(region)
      .withMaxRecords(1000) // default is 10000; avg situation is 2.5 KiB; situation stream carries 2 situations per record.
  }

  /**
    * Creates a new [[IRecordProcessor]] for [[Worker]]. Default is [[JacksonObjectMapperRecordProcessor]].
    * Override to change how messages are processed by this client.
    *
    * @return A new [[IRecordProcessor]]
    */
  override def createProcessor(): IRecordProcessor = new JacksonObjectMapperRecordProcessor

  /**
    * Start listening. Does nothing if already listing.
    */
  @PostConstruct
  def start(): Unit = synchronized {
    if (worker == null) {
      worker = new Worker.Builder()
        .recordProcessorFactory(this)
        .config(kinesisClientLibConfiguration)
        .build()

      Future {
        worker.run()
      } onComplete {
        case Success(_) ⇒
          log.info(s"Kinesis Client Library (stream=$streamName application=$applicationName) ended without error")
        case Failure(e) if NonFatal(e) ⇒
          onError(e)
        case _ ⇒
      }
    }
  }

  /**
    * Stop listening. Does nothing if not listening.
    */
  @PreDestroy
  def shutdown(): Unit = synchronized {
    if (worker != null) {
      worker.shutdown()
      log.info(s"Kinesis Client Library (stream=$streamName application=$applicationName) shutdown complete")
      ec.shutdown()
      worker = null
    }
  }

  /**
    * Map incoming JSON records to type [[T]] using a jackson [[ObjectMapper]]
    */
  class JacksonObjectMapperRecordProcessor extends IRecordProcessor {

    /**
      * The jackson mapper used to martial JSON into records of type [[T]].
      */
    val mapper = new ObjectMapper

    override def initialize(initializationInput: InitializationInput): Unit = {
      log.info(s"initialize (stream=$streamName application=$applicationName)")
      log.info(s"initializationInput.getShardId=${initializationInput.getShardId} (stream=$streamName application=$applicationName)")
      log.info(s"initializationInput.getExtendedSequenceNumber=${initializationInput.getExtendedSequenceNumber} (stream=$streamName application=$applicationName)")
    }

    override def shutdown(shutdownInput: ShutdownInput): Unit = {
      try {
        if (shutdownInput.getShutdownReason == ShutdownReason.TERMINATE) {
          onTerminate(checkpointFunctionFactory(shutdownInput.getCheckpointer))
        }
      } catch {
        case NonFatal(ex) ⇒
          onError(ex)
      }
    }

    override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
      // parse records from JSON to objects
      // log and throw out any failures
      val records = (for (record ← processRecordsInput.getRecords.asScala.toList) yield try {
        Some(mapper.readValue(record.getData.array, classObject))
      } catch {
        case NonFatal(ex) ⇒
          onError(ex)
          None
      }).flatten
      // pass parsed objects and checkpoint method to subclasses's parse method
      try {
        process(records, checkpointFunctionFactory(processRecordsInput.getCheckpointer))
      } catch {
        case NonFatal(ex) ⇒
          onError(ex)
      }
    }
  }

  private def checkpointFunctionFactory(checkpointer: IRecordProcessorCheckpointer): () ⇒ Unit = {
    () ⇒
      try {
        checkpointer.checkpoint()
      } catch {
        case NonFatal(ex) ⇒
          onError(ex)
      }
  }

}
