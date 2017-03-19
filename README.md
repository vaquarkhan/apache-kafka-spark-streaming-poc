# Spark Streaming Kafka consumer vaquar khan 

POC with Spark processing a stream from Kafka. Uses Log4j2 kafka appender.# Apache-kafka-spark-streaming-poc


https://stanford.edu/~rezab/sparkclass/slides/td_streaming.pdf

https://youtu.be/_adU0xpFpU8

---------------------------------------------------------------------------

http://blog.cloudera.com/blog/2015/03/exactly-once-spark-streaming-from-apache-kafka/

http://www.slideshare.net/prakash573/spark-streaming-best-practices

https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/README.html

-----------------------------------------------------------------------------------------------------

https://github.com/beeva/beeva-best-practices/blob/master/big_data/spark/README.md

https://www.infoq.com/articles/apache-spark-introduction

http://hortonworks.com/hadoop-tutorial/introduction-spark-streaming/

https://databricks.com/blog/2015/07/30/diving-into-apache-spark-streamings-execution-model.html

http://davidssysadminnotes.blogspot.com/2016/09/running-spark-streaming-dcos.html

------------------------------------------------------------------------------------------------------
FAQ:

https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/15%20Streaming%20FAQs.html

JIRA:
https://issues.apache.org/jira/browse/SPARK-18124

------------------------------------------------------------------------------------------------------

http://spark.apache.org/docs/latest/configuration.html


------------------------------------------------------------------------------------------------------

http://aseigneurin.github.io/


### Spark Streaming Backpressure. 

### Issue 

The streaming part works fine but when we initially start the job, we have to deal with really huge Kafka message backlog, millions of messages, and that first batch runs for over 40 hours,  and after 12 hours or so it becomes very very slow, it keeps crunching messages, but at a very low speed. 

### Solution 

spark.streaming.backpressure.enabled
spark.streaming.receiver.maxRate

https://vanwilgenburg.wordpress.com/2015/10/06/spark-streaming-backpressure/
