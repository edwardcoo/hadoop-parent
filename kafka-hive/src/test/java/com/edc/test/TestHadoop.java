package com.edc.test;

import org.junit.Test;

/**
 * Created by Mtime on 2017/9/28.
 */
public class TestHadoop {

    @Test
    public void testMobileHash(){
        String key = "18602488751";
        System.out.println(this.calcHash(key,8));
    }

    @Test
    public void testMemberNoHash(){
        String key = "C8660000000056802728";
        System.out.println(this.calcHash(key,8));
    }

    private int calcHash(String key,int shardCount){
        int hashCode = key.hashCode();
        int shard = hashCode % shardCount;
        return Math.abs(shard)+1;
    }

}
