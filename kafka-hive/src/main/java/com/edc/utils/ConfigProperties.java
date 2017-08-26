package com.edc.utils;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class ConfigProperties {
	protected final static Logger logger = LoggerFactory.getLogger(ConfigProperties.class);
	private static Properties properties;
	private static Object _lock = new Object();

	private ConfigProperties() {}

	private static Properties getProperties(){
		if (properties == null) {
			synchronized (_lock){
				if(properties == null){
					properties = load("conf.properties");
				}
			}
		}
		return properties;
	}

	private static Properties load(String filename) {
		Properties props = new Properties();
		try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
			 InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
			props.load(reader);
		} catch (Exception ex) {
			System.out.println("配置文件加载异常");
		}
		return props;
	}

	public static String get(String key) {
		return getProperties().getProperty(key);
	}

	public static String get(String key, String defaultValue) {
		return getProperties().getProperty(key, defaultValue);
	}

	public static void put(String key,String value){
		if(value == null){
			return;
		}
		getProperties().setProperty(key,value);
	}

	public static void main(String[] args) throws ConfigurationException {
		System.out.println(ConfigProperties.get("bootstrap.servers"));
//		 PropertiesUtil.instance().save();
	}

}
