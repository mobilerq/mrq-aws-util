package com.mobilerq.awsutil.kinesis

import org.joda.time.{DateTime, Duration}

import scala.collection.mutable

/**
  * Remove duplicates from a incoming batch using a key. Keeps the last record matching each key.
  *
  * @tparam KEY The key type
  * @tparam T   The target type
  */
abstract class BufferingKinesisConsumer[KEY, T <: AnyRef] extends AbstractKinesisConsumer[T] {

  def bufferMaxRecordCount: Int

  def bufferMaxRecordAge: Duration

  // call process method even without records (required to flush when bufferMaxRecordAge is exceeded)
  override def kinesisClientLibConfiguration =
    super.kinesisClientLibConfiguration.
      withCallProcessRecordsEvenForEmptyRecordList(true).
      withIdleTimeBetweenReadsInMillis(bufferMaxRecordAge.getMillis).
      withMaxRecords(bufferMaxRecordCount)

  override final def process(records: List[T], checkpoint: () ⇒ Unit): Unit = {
    val buffer = new mutable.HashMap[KEY, T]()
    for {
      record ← records
      key = recordToKey(record)
      timestamp = recordToTimestamp(record)
    } buffer.get(key) match {
      case None ⇒ buffer += key → record
      case Some(bufferedRecord) if timestamp isAfter recordToTimestamp(bufferedRecord) ⇒ buffer += key → record
      case _ ⇒ // ignore old records
    }
    if (buffer.nonEmpty) {
      flush(buffer.values.toList)
      checkpoint()
    }
  }

  /**
    * Implement this method to produce the key the record.
    * The buffer may contain at most one record per key.
    */
  def recordToKey(record: T): KEY

  /**
    * Implement this method to produce the timestamp of the record.
    * When there is a key collision, the record with the most recent key wins.
    */
  def recordToTimestamp(record: T): DateTime

  /**
    * Implement this method to handle a buffer of records that need to be flushed.
    * The underlying Kinesis stream is checkpointed after flush.
    */
  def flush(records: List[T]): Unit
}
