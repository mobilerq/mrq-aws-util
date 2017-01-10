package com.example.awsutil.dynamodb

import java.util.Collections

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ScanRequest, ScanResult}
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._

import scala.collection.JavaConverters._

class ScanResultIteratorTest {
  implicit val client = mock(classOf[AmazonDynamoDB])
  val requestCaptor = ArgumentCaptor.forClass(classOf[ScanRequest])

  @Test
  def testIteration(): Unit = {
    val items = Seq(
      Map("item" → new AttributeValue("1")),
      Map("item" → new AttributeValue("2")),
      Map("item" → new AttributeValue("3"))
    )
    when(client.scan(any(classOf[ScanRequest])))
      .thenReturn(new ScanResult()
        .withLastEvaluatedKey(items(1).asJava)
        .withItems(Seq(items(0).asJava, items(1).asJava).asJavaCollection)
        .withCount(2))
      .thenReturn(new ScanResult()
        .withLastEvaluatedKey(items(2).asJava)
        .withItems(Seq(items(2).asJava).asJavaCollection)
        .withCount(1))
      .thenReturn(new ScanResult()
        .withItems(Collections.emptyList[java.util.Map[String, AttributeValue]]())
        .withCount(0))
      .thenThrow(new RuntimeException("ouch"))
    val result = new ScanResultIterator(new ScanRequest).flatMap(_.getItems.asScala).map(_.asScala("item").getS).toList
    assertEquals(List("1", "2", "3"), result)
  }

}
