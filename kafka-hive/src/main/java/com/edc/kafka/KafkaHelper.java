package com.edc.kafka;

import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  2017/8/24 0024.
 */
public class KafkaHelper {
    protected static Logger logger = LoggerFactory.getLogger(KafkaHelper.class);
    private final static int timeOut = 10000;
    private final static int bufferSize = 60*1024;

    /**
     * 获取kafka logSize
     * @param topic
     * @param partitionMetadata 分区元数据
     * @return
     */
    public static long getKafkaLogSize(String topic,PartitionMetadata partitionMetadata){
        int partition = partitionMetadata.partitionId();
        String clientName = "Client_" + topic + "_" + partition;
//        Broker leaderBroker = partitionMetadata.leader();
        BrokerEndPoint leaderBroker = partitionMetadata.leader();
        if(leaderBroker == null){
            return 0L;
        }
        SimpleConsumer simpleConsumer = new SimpleConsumer(leaderBroker.host(), leaderBroker.port(), timeOut, bufferSize, clientName);
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = simpleConsumer.getOffsetsBefore(request);
        if (response.hasError()) {
            //System.out.println("Error fetching data Offset , Reason: " + response.errorCode(topic, partition) );
            return 0L;
        }
        long[] offsets = response.offsets(topic, partition);
        simpleConsumer.close();
//        System.out.println(clientName+"-logsize:"+offsets[0]);
        return offsets[0];
    }

    /**
     * 获取topic所有分区的 logSize总和
     * @param topic
     * @param partitionMetadatas 分区元数据
     * @return
     */
    private static long getKafkaTotalLogSize(String topic,List<PartitionMetadata> partitionMetadatas){
        if(StringUtils.isEmpty(topic) || partitionMetadatas==null){
            return 0L;
        }
        long logSize = 0L;
        for(PartitionMetadata metadata:partitionMetadatas){
            logSize+= getKafkaLogSize(topic,metadata);
        }
        return logSize;
    }

    /**
     * 获取 topic下的分区元数据
     * @param host
     * @param port
     * @param topic
     * @return
     */
    private static List<PartitionMetadata> getPartitionMetadatas(String host, int port, String topic) throws Exception{
        String clientName = "Client_Leader_LookUp";
        SimpleConsumer consumer = null;
        PartitionMetadata partitionMetaData = null;
        try {
            consumer = new SimpleConsumer(host, port, timeOut, bufferSize, clientName);
            List<String> topics = new ArrayList<String>();
            topics.add(topic);
            TopicMetadataRequest request = new TopicMetadataRequest(topics);
            TopicMetadataResponse reponse = consumer.send(request);
            List<TopicMetadata> topicMetadataList = reponse.topicsMetadata();
            if(topicMetadataList != null && topicMetadataList.get(0) != null){
                return topicMetadataList.get(0).partitionsMetadata();
            }
        } catch (Exception e) {
            logger.info("获取分区元数据异常",e);
            throw e;
        }finally {
            if(consumer!=null){
                consumer.close();
            }
        }
        return null;
    }

    /**
     * 从zookeeper中获取最后一条offset值(适合topic只有一个分区的情况)
     * @param connectString zookeeper连接信息
     * @param topic
     * @param groupId
     * @param partitionMetadatas
     * @return
     * @throws Exception
     */
    public static long getZookeeperOffset(String connectString,String topic,String groupId,List<PartitionMetadata> partitionMetadatas){
        if(StringUtils.isEmpty(topic) || partitionMetadatas==null){
            return 0L;
        }
        long offset = 0L;
        try {
            ZooKeeper zookeeper = new ZooKeeper(connectString, 2000, new StubWatcher());
            //从zookeeper的这个路径获取offset
            //String path = "/consumers/groupid/offsets/customer_base_group/0";
            String path = "/consumers/{}/offsets/{}/{}";
            for(PartitionMetadata metadata:partitionMetadatas){
                String partitionPath = MessageFormatter.arrayFormat(path,new Object[]{groupId,topic,metadata.partitionId()}).getMessage();
                byte[] data = zookeeper.getData(partitionPath, false, null);
                offset+= Long.valueOf(new String(data));
            }
            zookeeper.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return offset;
    }

    /**
     *  获取lag
     * @param kafkaConnect  kafka链接
     * @param zkConnect  zookeeper 链接
     * @param topic
     * @param groupId
     * @return
     * @throws Exception
     */
    public static long getLag(String kafkaConnect,String zkConnect,String topic,String groupId) throws Exception {
        try {
            String[] connects = kafkaConnect.split(",");
            String[] kafkaProperties = connects[0].split(":");
            String host = kafkaProperties[0];
            int port = Integer.valueOf(kafkaProperties[1]);
            //获取topic下的分区元数据列表
            List<PartitionMetadata> partitionMetadatas = getPartitionMetadatas(host, port, topic);
            //获取kafka总的logsize
            long kafkaLogSize = getKafkaTotalLogSize(topic,partitionMetadatas);
            //获取zookeeper总的offset
            long zkOffset = getZookeeperOffset(zkConnect,topic,groupId,partitionMetadatas);
            logger.info("检测到{}条消息未消费",(kafkaLogSize-zkOffset));
            return kafkaLogSize-zkOffset;
        } catch (Exception e) {
            logger.info("获取Lag异常",e);
           throw e;
        }
    }

    /**
     * 测试
     * @param args
     * @throws Exception
     */
    public static void main(String [] args) throws Exception {
        String kafkaConnect = "192.168.52.160:9092";
        String zookeeperConnect = "bidev191:2181,bidev192:2181,bidev193:2181";
        String topic = "customer_base_group";
        String groupId = "customer_group";
        //Lag=log_size - zk_offset,如果lag值不为0则表示有kafka中存在未消费的记录
        try {
            System.out.println("Lag："+getLag(kafkaConnect,zookeeperConnect,topic,groupId));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit( 5);
        }

    }

}