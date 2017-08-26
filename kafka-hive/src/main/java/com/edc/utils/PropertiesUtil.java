package com.edc.utils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtil {
	protected final static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);
	private static PropertiesUtil propertiesUtil = null;
	private PropertiesConfiguration config = null;

	private PropertiesUtil() {
		try {
			config = new PropertiesConfiguration("conf.properties");
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static PropertiesUtil instance() {
		if (propertiesUtil == null) {
			propertiesUtil = new PropertiesUtil();
		}
		return propertiesUtil;
	}

	
	public String getVal(String key) {
		return config.getString(key);
	}
	public int getValInt(String key) {
		return config.getInt(key);
	}
	
	public String getVal(String key, String defaultValue) {
		return config.getString(key, defaultValue);
	}
//	public void add(String key,String val){
//		config.addProperty(key, val);
//	
//	}
//	public void set(String key,String val){
//		config.setProperty(key, val);
//	
//	}
//	public void save() throws ConfigurationException{
//		config.save();
//	}
	public static void main(String[] args) throws ConfigurationException {
		System.out.println(PropertiesUtil.instance().getVal("hive.meta.mysql.ip"));
//		 PropertiesUtil.instance().save();
	}

}
