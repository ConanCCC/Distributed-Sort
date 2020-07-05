import java.io.IOException; 
import java.util.Iterator; 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
import org.apache.hadoop.util.Tool; 
import org.apache.hadoop.util.ToolRunner; 
import org.slf4j.Logger; 
import org.slf4j.LoggerFactory; 


public class Sort extends Configured  implements Tool { 
    private static final Logger logger = LoggerFactory.getLogger(Sort.class); 
    public static class SortMapper extends Mapper<Text, Text, CombinationKey, IntWritable> { 
    //--------------------------------------------------------- 
        CombinationKey combinationKey = new CombinationKey(); 
        Text sortName = new Text(); 
        IntWritable score = new IntWritable(); 
        String[] inputString = null; 
    //--------------------------------------------------------- 
        @Override
        protected void map(Text keys, Text values, Context context) 
                throws IOException, InterruptedException { 
            logger.info("---------enter map function flag---------"); 
            //过滤非法记录 
            if(keys == null || values == null || keys.toString().equals("") 
                    || values.equals("")){ 
                return; 
            } 
            String line = keys.toString();
            // 将String划分
            String[] gens = line.split(" ");
            String key = gens[0];
            String value = gens[1];
            if (key.equals("") || value.equals("")) return;
            sortName.set(key); 
            score.set(Integer.parseInt(value)); 
            combinationKey.setFirstKey(sortName); 
            combinationKey.setSecondKey(score); 
            //map输出 
            context.write(combinationKey, score); 
            logger.info("---------out map function flag---------"); 
        } 
    } 
    public static class SortReducer extends
    Reducer<CombinationKey, IntWritable, Text, Text> { 
        StringBuffer sb = new StringBuffer(); 
        Text sore = new Text(); 
        @Override
        protected void reduce(CombinationKey key, 
                Iterable<IntWritable> value, Context context) 
                throws IOException, InterruptedException { 
            sb.delete(0, sb.length());//先清除上一个组的数据 
            Iterator<IntWritable> it = value.iterator(); 
                                                    
            while(it.hasNext()){ 
                sb.append(it.next()+","); 
            } 
            //去除最后一个逗号 
            if(sb.length()>0){ 
                sb.deleteCharAt(sb.length()-1); 
            } 
            sore.set(sb.toString()); 
            context.write(key.getFirstKey(),sore); 
            logger.info("---------enter reduce function flag---------"); 
            logger.info("reduce Input data:{["+key.getFirstKey()+","+ 
            key.getSecondKey()+"],["+sore+"]}"); 
            logger.info("---------out reduce function flag---------"); 
        } 
    } 
    @Override
    public int run(String[] args) throws Exception { 
        Configuration conf=getConf(); //获得配置文件对象 
        Job job=new Job(conf,"SoreSort"); 
        job.setJarByClass(Sort.class); 

		FileSystem fs = FileSystem.get(conf);                                                
        FileInputFormat.addInputPath(job, new Path(args[0])); //设置map输入文件路径
        Path path = new Path(args[1]);

        if(fs.exists(path))//如果目录存在，则删除目录
        {
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job, path);
        //FileOutputFormat.setOutputPath(job, new Path(args[1])); //设置reduce输出文件路径 
                                                                                                                                                                                    
        job.setMapperClass(SortMapper.class); 
        job.setReducerClass(SortReducer.class); 
                                                
        job.setPartitionerClass(DefinedPartition.class); //设置自定义分区策略 
                                                                                                                                                                                    
        job.setGroupingComparatorClass(DefinedGroupSort.class); //设置自定义分组策略 
        job.setSortComparatorClass(DefinedComparator.class); //设置自定义二次排序策略 
                                              
        job.setInputFormatClass(KeyValueTextInputFormat.class); //设置文件输入格式 
        job.setOutputFormatClass(TextOutputFormat.class);//使用默认的output格式 
                                                
        //设置map的输出key和value类型 
        job.setMapOutputKeyClass(CombinationKey.class); 
        job.setMapOutputValueClass(IntWritable.class); 
                                                
        //设置reduce的输出key和value类型 
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(Text.class); 
        job.waitForCompletion(true); 
        return job.isSuccessful()?0:1; 
    } 
                                            
    public static void main(String[] args) { 
        try { 
            int returnCode =  ToolRunner.run(new Sort(),args); 
            System.exit(returnCode); 
        } catch (Exception e) { 
            // TODO Auto-generated catch block 
            e.printStackTrace(); 
        } 
                                                
    } 
}