package org.apache.spark.streaming.kafka

import com.alibaba.fastjson.{JSON, TypeReference}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaCluster.Err

import scala.reflect.ClassTag

/**
  * @Description kafka工具类
  * @author gs
  * @date 2020/2/3-12:42 
  */
class KafkaManager(val kafkaParams:Map[String,String],
                   isUpdate:Boolean) extends Serializable with Logging {

  val kafkaCluster = new KafkaCluster(kafkaParams)

  def createJson2MapStringDricetStreamWithOffset(ssc:StreamingContext,
   topicSet:Set[String]):DStream[java.util.Map[String,String]] = {
    val converter = {
      json:String=>{
        var res: java.util.Map[String,String] = null
        try {
          res = com.alibaba.fastjson.JSON.parseObject(
            json, new TypeReference[java.util.Map[String, String]]() {}
          )
        }catch {
          case e => logError("转换失败====="+json)
        }
        res
      }
    }
    createDirectStream[String, String, StringDecoder, StringDecoder](ssc,topicSet)
      .map(tuple=>converter(tuple._2))
  }
  def createDirectStream[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag](ssc: StreamingContext,
                              topics: Set[String]): InputDStream[(K, V)] = {
    val groupId = kafkaParams.get("group.id").getOrElse("default")
    setOrUpdateOffsets(topics,groupId)
    //TODO  获取消费者组的偏移
    // 获取kafka流
    val messages:InputDStream[(K, V)] ={
      //TODO 获取partion 的分区信息
      val PartitionsE:Either[Err, Set[TopicAndPartition]] = kafkaCluster.
        getPartitions(Set("chl_test2"))
      val right = PartitionsE.right
      val left = PartitionsE.left
      println("=========" + PartitionsE.isLeft)
      //断言，如果正确，继续执行，如果错误，抛出异常，程序中断
      require(PartitionsE.isRight,s"获取partions失败")
      val partitions:Set[TopicAndPartition] = PartitionsE.right.get
      println("打印分区信息:")
      partitions.foreach(println(_))

      //拿消费者组的信息
      //TODO 获取消费者组的偏移
      val consumerOffsetsE = kafkaCluster.getConsumerOffsets(
        "console-consumer-92836",partitions)
      val consumerOffsets = consumerOffsetsE.right.get
      println("打印消费者的偏移")
      consumerOffsets.foreach(println(_))
      //TODO 从消费者组的偏移开始消费
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc,
        kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (
          mmd.key(), mmd.message()))
    }
    //TODO 往ZK里面去更新
    if (true){
      //从spark的 DS中拿到偏移
      messages.foreachRDD(rdd=>{
        println("RDD消费成功,更新ZK中的offsets")
        updateZKOffsers(rdd)
      })
    }
    //把流最后返回
    messages
  }



  //更新ZK中的offsets
  def updateZKOffsers[K:ClassTag,V:ClassTag](rdd: RDD[(K, V)]): Unit = {
    val groupId = kafkaParams.get("group.id").getOrElse("default")
    //拿到spark中的全部偏移
    val offsetList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetList.foreach(x=>{
      println(s"获取kafka中的偏移信息${x}" )
    })
    //遍历偏移，记录到zk中
    offsetList.foreach(offset=>{
      //TODO 构造当前的偏移信息
      val topicAndPartition = TopicAndPartition(offset.topic,offset.partition)
      //Map[TopicAndPartition, Long]
      //更新偏移信息到zk
      setOffsets(groupId,Map((topicAndPartition,offset.untilOffset)))
      rdd.foreach(println(_))
    })
  }
  def setOffsets(groupId: String,
                 offsets: Map[TopicAndPartition, Long]): Unit = {
    if (offsets.nonEmpty){
      val value = kafkaCluster.setConsumerOffsets(groupId,offsets)
      if(value.isLeft){
        logError("更新偏移异常")
      }
    }
  }

  /**
    * 创建数据流前，根据实际消费情况更新消费offsets
    * @param topics
    * @param groupId
    */
  private def setOrUpdateOffsets(topics: Set[String],
                                 groupId: String):Unit= {
    topics.foreach(topic => {
      //获取kafka  partions的节点信息
      val partitionsE = kafkaCluster.getPartitions(Set(topic))
      logInfo(partitionsE+"")
      //检测
      require(partitionsE.isRight, s"获取 kafka topic ${topic}`s partition 失败。")
      val partitions = partitionsE.right.get

      //获取最早的 partions offsets信息
      val earliestLeader = kafkaCluster.getEarliestLeaderOffsets(partitions)
      val earliestLeaderOffsets = earliestLeader.right.get
      println("kafka中最早的消息偏移")
      earliestLeaderOffsets.foreach(println(_))

      //获取最末的 partions offsets信息
      val latestLeader = kafkaCluster.getLatestLeaderOffsets(partitions)
      val latestLeaderOffsets = latestLeader.right.get
      println("kafka中最末的消息偏移")
      latestLeaderOffsets.foreach(println(_))

      //获取消费者组的 offsets信息
      val consumerOffsetsE = kafkaCluster.getConsumerOffsets(groupId, partitions)
      //如果消费者offset存在
      if (consumerOffsetsE.isRight){
        //TODO 说明消费者组已经存在了
        /**
          * 如果zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        //如果earliestLeader 存在
        if (earliestLeader.isRight){
          //获取最早的offset 也就是最小的offset
          val earliestLeaderOffsets = earliestLeader.right.get
          //获取消费者组的offset
          val consumerOffsets = consumerOffsetsE.right.get
          // 将 consumerOffsets 和 earliestLeaderOffsets 的offsets 做比较
          // 可能只是存在部分分区consumerOffsets过时，
          // 所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
          var offsets: Map[TopicAndPartition, Long] = Map()
          consumerOffsets.foreach({ case (tp, n) =>
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            //如果消費者的偏移小于 kafka中最早的offset,那麽，將最早的offset更新到zk
            if (n < earliestLeaderOffset){
              logWarning("consumer group:" + groupId + ",topic:" + tp.topic +
                ",partition:" + tp.partition +
                " offsets已经过时，更新为" + earliestLeaderOffset)
              offsets += (tp -> earliestLeaderOffset)
            }
          })
          //设置offsets
          setOffsets(groupId, offsets)
        }
      }else {
        // 消费者还没有消费过  也就是zookeeper中还没有消费者的信息
        if(earliestLeader.isLeft)
          logError(s"${topic} hasConsumed but earliestLeaderOffsets is null。")
        //看是从头消费还是从末开始消费  smallest表示从头开始消费
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
          .getOrElse("smallest")
        //构建消费者 偏移
        var leaderOffsets: Map[TopicAndPartition, Long] = Map.empty
        //从头消费
        if (reset.equals("smallest")){
          //分为 存在 和 不存在 最早的消费记录 两种情况
          //如果kafka 最小偏移存在，则将消费者偏移设置为和kafka偏移一样
          if (earliestLeader.isRight){
            leaderOffsets = earliestLeader.right.get.map{
              case (tp, offset) => (tp, offset.offset)
            }
          }else{
            // 如果不存在，则从新构建偏移全部为0 offsets
            leaderOffsets = partitions.map(tp => (tp, 0L)).toMap
          }
        }else{
          //直接获取最新的offset
          leaderOffsets = kafkaCluster.getLatestLeaderOffsets(partitions).
            right.get.map{
            case (tp, offset) => (tp, offset.offset)
          }
        }
        //设置offsets
        setOffsets(groupId, leaderOffsets)
      }
    })

  }
}
