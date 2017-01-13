package com.mobilrq.awsutil.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ListNextBatchOfObjectsRequest, ListObjectsRequest, ObjectListing}

/**
  * Iterate over the results of a [[ListObjectsRequest]]
  */
class ListObjectIterator(request: ListObjectsRequest)(implicit client: AmazonS3) extends Iterator[ObjectListing] {

  def this(bucket: String)(implicit client: AmazonS3) =
    this(new ListObjectsRequest().withBucketName(bucket))

  def this(bucket: String, prefix: String)(implicit client: AmazonS3) =
    this(new ListObjectsRequest().withBucketName(bucket).withPrefix(prefix))

  var last: ObjectListing = _

  override def hasNext: Boolean = last == null || last.isTruncated

  override def next(): ObjectListing = {
    if (!hasNext) throw new IllegalStateException("empty iterator")
    last = if (last == null) {
      client.listObjects(request)
    } else {
      client.listNextBatchOfObjects(new ListNextBatchOfObjectsRequest(last)
        .withGeneralProgressListener[ListNextBatchOfObjectsRequest](request.getGeneralProgressListener)
        .withRequestMetricCollector[ListNextBatchOfObjectsRequest](request.getRequestMetricCollector)
        .withSdkClientExecutionTimeout[ListNextBatchOfObjectsRequest](Option(request.getSdkClientExecutionTimeout).map(_.intValue()).getOrElse(0))
        .withSdkRequestTimeout[ListNextBatchOfObjectsRequest](Option(request.getSdkRequestTimeout).map(_.intValue()).getOrElse(0))
      )
    }
    last
  }

}
