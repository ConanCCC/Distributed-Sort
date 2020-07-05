//package com.conanc; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.io.WritableComparator; 
import org.slf4j.Logger; 
import org.slf4j.LoggerFactory; 
/** 
 * 自定义二次排序策略 
 */
public class DefinedComparator extends WritableComparator { 
    private static final Logger logger = LoggerFactory.getLogger(DefinedComparator.class); 
    public DefinedComparator() { 
        super(CombinationKey.class,true); 
    } 
    @Override
    public int compare(WritableComparable combinationKeyOne, 
            WritableComparable CombinationKeyOther) { 
        logger.info("---------enter DefinedComparator flag---------"); 
                                                      
        CombinationKey c1 = (CombinationKey) combinationKeyOne; 
        CombinationKey c2 = (CombinationKey) CombinationKeyOther; 
        // 确保进行排序的数据在同一个区内，如果不在同一个区则按照组合键中第一个键排序 
        
        if(!c1.getFirstKey().equals(c2.getFirstKey())){ 
            logger.info("---------out DefinedComparator flag---------"); 
            return c1.getFirstKey().compareTo(c2.getFirstKey()); 
            } 
        else{
            //按照组合键的第二个键的升序排序，将c1和c2倒过来则是按照数字的降序排序(假设2) 
            logger.info("---------out DefinedComparator flag---------"); 
            return c1.getSecondKey().get()-c2.getSecondKey().get();//0,负数,正数 
        }
    } 
}