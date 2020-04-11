package org.apache.spark.streaming.kafka
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}
/**
  * @Description TODO
  * @author gs
  * @date 2020/2/2-23:27
  */
object KafkaClusterTest {

  def main(args: Array[String]): Unit = {
    val kafkaCluster = new KafkaCluster(kafkaParams)
    //PartitionsE 结构里面有2个结构，一个是left,一个是right
    //TODO  kafka的所有partion信息
    val PartitionsE:Either[Err, Set[TopicAndPartition]] =
      kafkaCluster.getPartitions(Set("chl_test2"))
    val left = PartitionsE.isLeft
    val right = PartitionsE.isRight
    println("=========" + PartitionsE.isLeft)
    //断言，如果正确，继续执行，如果错误，抛出异常，程序中断
    require(PartitionsE.isRight,s"获取partions失败")
    val partitions:Set[TopicAndPartition] = PartitionsE.right.get
    println("打印分区信息:")
    partitions.foreach(println(_))
    //TODO 获取topic的最小的偏移
    val earliestLeaderOffsetsE:Either[Err,
      Map[TopicAndPartition, LeaderOffset]] = kafkaCluster.
      getEarliestLeaderOffsets(partitions)
    val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
    println("打印最小的偏移")
    earliestLeaderOffsets.foreach(println(_))
    //TODO 获取topic的最大的偏移
    val latestLeaderOffsetsE:Either[Err,
      Map[TopicAndPartition, LeaderOffset]] = kafkaCluster.
      getLatestLeaderOffsets(partitions)
    val latestLeaderOffsets = latestLeaderOffsetsE.right.get
    println("打印最大的偏移")
    latestLeaderOffsets.foreach(println(_))
    //TODO 获取消费者组的偏移
    val consumerOffsetsE = kafkaCluster.getConsumerOffsets(
      "console-consumer-92836",partitions)
    val consumerOffsets = consumerOffsetsE.left.get
    println("打印消费者的偏移")
    consumerOffsets.foreach(println(_))
  }

  val kafkaParams:Map[String,String] = Map[String,String](
    "metadata.broker.list"->"cdh12:9092",
    "group.id"->"console-consumer-92836",
    "auto.offset.reset"->"smallest"
  )

}
