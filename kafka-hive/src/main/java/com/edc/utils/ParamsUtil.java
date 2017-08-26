package com.edc.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Mtime on 2017/8/26.
 */
public class ParamsUtil {

    private ParamsUtil(){}
    private final static  String KEY_KAFKA = "params_kafka";
    private final static Map<String,CommandLine> commandLineMap = new HashMap<>();
    public final static String SEPERATOR = "~";
    public final static String KAFKA_TOPIC = "topic";
    public final static String KAFKA_GROUPID = "groupid";
    public final static String KAFKA_CONNECT = "kafka_connect";
    public final static String KAFKA_AUTO_OFFSET_RESET = "auto_offset_reset";
    public final static String ZOOKEEPER_CONNECT = "zookeeper_connect";

    public static  Map<String,CommandLine> put(String paramType,CommandLine value){
        commandLineMap.put(paramType,value);
        return commandLineMap;
    }

    public static  CommandLine get(String paramType){
        return commandLineMap.get(paramType);
    }

    public static String getParamValue(String paramType,String paramKey){
        CommandLine commandLine = get(paramType);
        if(commandLine == null){
            return null;
        }
        return commandLine.getOptionValue(paramKey);
    }

    public static Map<String,CommandLine> putKafka(CommandLine commandLine){
        put(KEY_KAFKA,commandLine);
        return commandLineMap;
    }

    public static String getKafkaParam(String key){
        return getParamValue(KEY_KAFKA,key);
    }
    public static String getKafkaParam(String key,String defaultValue){
        String value = getKafkaParam(key);
        if(StringUtils.isEmpty(value)){
            return defaultValue;
        }
        return value;
    }

    public static boolean isEmptyKafkaParam(String key){
        return StringUtils.isEmpty(getKafkaParam(key));
    }

    public static boolean isEmpty(CommandLine commandLine, String optionKey){
        return !commandLine.hasOption(optionKey) || StringUtils.isEmpty(commandLine.getOptionValue(optionKey));
    }

}
