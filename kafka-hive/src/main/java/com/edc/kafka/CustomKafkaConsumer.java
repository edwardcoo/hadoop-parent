package com.edc.kafka;

import com.edc.utils.ConfigProperties;
import com.edc.utils.ParamsUtil;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Mtime on 2017/8/23.
 */
public class CustomKafkaConsumer {

    protected static Logger logger = LoggerFactory.getLogger(CustomKafkaConsumer.class);

    public String consumeOne(String zookeeperConnect,String topic,String groupId){
        ConsumerConnector consumer = initConsumer(zookeeperConnect,groupId);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,
                keyDecoder, valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
        String msg="";
        try {
            ConsumerIterator<String, String> it = stream.iterator();
            if(it.hasNext()){
                RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
                String name = runtime.getName();
                msg = it.next().message();
                consumer.commitOffsets();
                logger.info("消息处理成功, 进程名称为: {},进程id为:{} , 内容: {}",
                        name,Integer.parseInt(name.substring(0, name.indexOf("@"))), msg);
            }
        } catch (Exception e) {
            logger.warn("消息处理失败,内容: {} , 异常:{}", msg , e);
        }finally {
            consumer.shutdown();
        }
        return msg;
    }

    private ConsumerConnector initConsumer(String zookeeperConnect,String groupId) {
        Properties props = new Properties();
        // zookeeper配置
        //, "biqa160:2181,biqa161:2181,biqa162:2181"
        props.put("zookeeper.connect",zookeeperConnect);
        // group 代表一个消费组
        props.put("group.id", groupId);
        // zk超时配置
        props.put("zookeeper.session.timeout.ms", ConfigProperties.get("zookeeper.session.timeout.ms", "1000"));
        props.put("zookeeper.sync.time.ms", ConfigProperties.get("zookeeper.sync.time.ms", "1000"));
        props.put("auto.commit.enable", "true");
        props.put("auto.commit.interval.ms","1000");
        props.put("auto.offset.reset", ParamsUtil.getKafkaParam(ParamsUtil.KAFKA_AUTO_OFFSET_RESET,"smallest"));
        // 序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ConsumerConfig config = new ConsumerConfig(props);
        return kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }


}
