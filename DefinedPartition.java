//package com.conanc; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.mapreduce.Partitioner; 
import org.slf4j.Logger; 
import org.slf4j.LoggerFactory; 
/** 
 * 自定义分区 
 */
public class DefinedPartition extends Partitioner<CombinationKey,IntWritable>{ 
    private static final Logger logger = LoggerFactory.getLogger(DefinedPartition.class); 
    /** 
    *  数据输入来源：map输出 
    * @param key map输出键值 
    * @param value map输出value值 
    * @param numPartitions 分区总数，即reduce task个数 
    */
    @Override
    public int getPartition(CombinationKey key, IntWritable value,int numPartitions) { 
        logger.info("--------enter DefinedPartition flag--------"); 
        /** 
        * 注意：这里采用默认的hash分区实现方法 
        * 根据组合键的第一个值作为分区
        */
        logger.info("--------out DefinedPartition flag--------"); 
        return (key.getFirstKey().hashCode()&Integer.MAX_VALUE)%numPartitions; 
    } 
}