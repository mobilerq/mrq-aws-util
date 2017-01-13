package com.mobilerq.awsutil.dynamodb

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{ScanRequest, ScanResult}


/**
  * Iterate over results of a [[ScanRequest]].
  *
  * @param request The request. Each iteration clones this request and updates the exclusive start key using the
  *                previous result.
  * @param client  the client.
  */
class ScanResultIterator(request: ScanRequest)(implicit client: AmazonDynamoDB) extends Iterator[ScanResult] {

  var nextFlag = true
  var lastKey = request.getExclusiveStartKey

  override def hasNext: Boolean = nextFlag

  override def next(): ScanResult = {
    if (!nextFlag) throw new IllegalStateException("empty iterator")
    val result = client.scan(request.clone.withExclusiveStartKey(lastKey))
    lastKey = result.getLastEvaluatedKey
    nextFlag = lastKey != null && !lastKey.isEmpty
    result
  }
}