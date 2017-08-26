package com.edc.kafka;

import com.edc.utils.ParamsUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Mtime on 2017/8/23.
 */
public class GetDataFromKafka {

    protected static Logger logger = LoggerFactory.getLogger(GetDataFromKafka.class);

    public static CommandLine parseCommandLine(String[] args) throws org.apache.commons.cli.ParseException {
        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption(ParamsUtil.KAFKA_TOPIC, "kafka.topic", true, "kafka topic");
        options.addOption(ParamsUtil.KAFKA_GROUPID, "kafka.groupid", true, "kafka groupid");
        options.addOption(ParamsUtil.KAFKA_CONNECT, "kafka.connect", true, "kafka connect");
        options.addOption(ParamsUtil.ZOOKEEPER_CONNECT, "zookeeper.connect", true, "zookeeper connect");
        options.addOption(ParamsUtil.KAFKA_AUTO_OFFSET_RESET, "auto.offset.reset", true, "从开始（smallest）消费还是从末尾（largest）消费");
        return parser.parse(options, args);
    }

    private static boolean validateParam(CommandLine commandLine){
        if(ParamsUtil.isEmptyKafkaParam(ParamsUtil.KAFKA_TOPIC)){
            logger.error("kafka topic 参数不能为空");
            return false;
        }
        if(ParamsUtil.isEmptyKafkaParam(ParamsUtil.KAFKA_GROUPID)){
            logger.error("kafka groupid 参数不能为空");
            return false;
        }
        if(ParamsUtil.isEmptyKafkaParam(ParamsUtil.KAFKA_CONNECT)){
            logger.error("kafka connect 参数不能为空");
            return false;
        }
        if(ParamsUtil.isEmptyKafkaParam(ParamsUtil.ZOOKEEPER_CONNECT)){
            logger.error("zookeeper connect 参数不能为空");
            return false;
        }
        return true;
    }

    public static void main(String[] args){
        //exit 1:参数不正确，2：没有kafka消息,5:获取kafka消息异常
        try {
            CommandLine commandLine = parseCommandLine(args);
            ParamsUtil.putKafka(commandLine);
            if(!validateParam(commandLine)){
                System.exit(1);
            }
            //-topic customer_base_group -groupid customer_group -zookeeper_connect bidev191:2181,bidev192:2181,bidev193:2181 -kafka_connect 192.168.52.160:9092
            String topic = commandLine.getOptionValue(ParamsUtil.KAFKA_TOPIC);
            String groupId = commandLine.getOptionValue(ParamsUtil.KAFKA_GROUPID);
            String kafkaConnect = commandLine.getOptionValue(ParamsUtil.KAFKA_CONNECT);
            String zookeeperConnect = commandLine.getOptionValue(ParamsUtil.ZOOKEEPER_CONNECT);
            long lag = KafkaHelper.getLag(kafkaConnect, zookeeperConnect, topic, groupId);
            if(lag <= 0){
                logger.info("从kafka未消费到消息");
                System.exit(2);
            }
            CustomKafkaConsumer consumer = new CustomKafkaConsumer();
            String msg = consumer.consumeOne(zookeeperConnect,topic, groupId);
            System.out.println(lag+ParamsUtil.SEPERATOR+msg);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(5);
        }
    }

}
