package com.mobilrq.awsutil.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}

/**
  * Iterate over the results of a [[ListObjectsV2Request]]. Calls request withContinuationToken each iteration.
  */
class ListObjectV2Iterator(request: ListObjectsV2Request)(implicit client: AmazonS3) extends Iterator[ListObjectsV2Result] {

  def this(bucket: String)(implicit client: AmazonS3) =
    this(new ListObjectsV2Request().withBucketName(bucket))

  def this(bucket: String, prefix: String)(implicit client: AmazonS3) =
    this(new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix))

  var token = request.getContinuationToken
  var isTruncated = true

  override def hasNext: Boolean = isTruncated

  override def next(): ListObjectsV2Result = {
    if (!hasNext) throw new IllegalStateException("empty iterator")
    val result = client.listObjectsV2(request.withContinuationToken(token))
    isTruncated = result.isTruncated
    token = result.getNextContinuationToken
    result
  }

}
