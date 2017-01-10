package com.example.awsutil.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, S3ObjectSummary}
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._

import scala.collection.JavaConverters._

class ListObjectV2IteratorTest {

  implicit val client = mock(classOf[AmazonS3])
  val requestCaptor = ArgumentCaptor.forClass(classOf[ListObjectsV2Request])

  @Test
  def testBucketConstructor(): Unit = {
    when(client.listObjectsV2(any(classOf[ListObjectsV2Request]))).thenReturn(new ListObjectsV2Result)
    new ListObjectV2Iterator("bucket").next()
    verify(client).listObjectsV2(requestCaptor.capture())
    assertEquals("bucket", requestCaptor.getValue.getBucketName)
  }

  @Test
  def testBucketPrefixConstructor(): Unit = {
    when(client.listObjectsV2(any(classOf[ListObjectsV2Request]))).thenReturn(new ListObjectsV2Result)
    new ListObjectV2Iterator("bucket", "prefix").next()
    verify(client).listObjectsV2(requestCaptor.capture())
    assertEquals("bucket", requestCaptor.getValue.getBucketName)
    assertEquals("prefix", requestCaptor.getValue.getPrefix)
  }

  @Test
  def testRequestConstructor(): Unit = {
    when(client.listObjectsV2(any(classOf[ListObjectsV2Request]))).thenReturn(new ListObjectsV2Result)
    new ListObjectV2Iterator(new ListObjectsV2Request()
      .withBucketName("bucket")
      .withPrefix("prefix")
      .withContinuationToken("t")).next()
    verify(client).listObjectsV2(requestCaptor.capture())
    assertEquals("bucket", requestCaptor.getValue.getBucketName)
    assertEquals("prefix", requestCaptor.getValue.getPrefix)
    assertEquals("t", requestCaptor.getValue.getContinuationToken)
  }

  @Test
  def testIteration(): Unit = {
    when(client.listObjectsV2(any(classOf[ListObjectsV2Request])))
      .thenReturn({
        val r = new ListObjectsV2Result()
        r.setTruncated(true)
        r.setContinuationToken("a")
        r.getObjectSummaries.add({
          val s = new S3ObjectSummary()
          s.setKey("1")
          s
        })
        r.getObjectSummaries.add({
          val s = new S3ObjectSummary()
          s.setKey("2")
          s
        })
        r
      }).thenReturn({
      val r = new ListObjectsV2Result()
      r.setTruncated(false)
      r.getObjectSummaries.add({
        val s = new S3ObjectSummary()
        s.setKey("3")
        s
      })
      r
    }).thenThrow(new RuntimeException("ouch"))
    val result = new ListObjectV2Iterator("bucket").flatMap(_.getObjectSummaries.asScala).map(_.getKey).toList
    assertEquals(List("1", "2", "3"), result)
  }

}
