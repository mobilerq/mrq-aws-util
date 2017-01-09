package com.example.awsutil.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model._
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._

class ListObjectIteratorTest {

  implicit val client = mock(classOf[AmazonS3])
  val requestCaptor = ArgumentCaptor.forClass(classOf[ListObjectsRequest])
  val nextRequestCaptor = ArgumentCaptor.forClass(classOf[ListNextBatchOfObjectsRequest])

  @Test
  def testBucketConstructor(): Unit = {
    when(client.listObjects(any(classOf[ListObjectsRequest]))).thenReturn(new ObjectListing)
    new ListObjectIterator("bucket").next()
    verify(client).listObjects(requestCaptor.capture())
    assertEquals("bucket", requestCaptor.getValue.getBucketName)
  }

  @Test
  def testBucketPrefixConstructor(): Unit = {
    when(client.listObjects(any(classOf[ListObjectsRequest]))).thenReturn(new ObjectListing)
    new ListObjectIterator("bucket", "prefix").next()
    verify(client).listObjects(requestCaptor.capture())
    assertEquals("bucket", requestCaptor.getValue.getBucketName)
    assertEquals("prefix", requestCaptor.getValue.getPrefix)
  }

  @Test
  def testRequestConstructor(): Unit = {
    when(client.listObjects(any(classOf[ListObjectsRequest]))).thenReturn(new ObjectListing)
    new ListObjectIterator(new ListObjectsRequest()
      .withBucketName("bucket")
      .withPrefix("prefix")).next()
    verify(client).listObjects(requestCaptor.capture())
    assertEquals("bucket", requestCaptor.getValue.getBucketName)
    assertEquals("prefix", requestCaptor.getValue.getPrefix)
  }

  @Test
  def testIteration(): Unit = {
    import scala.collection.JavaConverters._
    val first = {
      val r = new ObjectListing()
      r.setTruncated(true)
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
    }
    val second = {
      val r = new ObjectListing()
      r.setTruncated(false)
      r.getObjectSummaries.add({
        val s = new S3ObjectSummary()
        s.setKey("3")
        s
      })
      r
    }
    when(client.listObjects(any(classOf[ListObjectsRequest]))).thenReturn(first)
    when(client.listNextBatchOfObjects(nextRequestCaptor.capture())).thenReturn(second).thenThrow(new RuntimeException("ouch"))
    val result = new ListObjectIterator("bucket")
      .flatMap(_.getObjectSummaries.asScala)
      .map(_.getKey)
      .toList
    assertEquals(first, nextRequestCaptor.getValue.getPreviousObjectListing)
    assertEquals(List("1", "2", "3"), result)
  }

}
