# AWS Util for DynamoDB

## Iterators

This provides iterators for DynamoDB items.

Example
   
```
import scala.collection.JavaConverters._

implicit val client =  new AmazonDynamoDBClient
for {
  result ← new ScanResultIterator(new ScanRequest("my_table"))
  item ← result.getItems.asScala
} {
  println(item)
}
```
