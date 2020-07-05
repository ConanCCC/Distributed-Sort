# MapReduce进行分布式排序

## 项目说明

本项目为云计算概论实验期末设计，实现了使用MapReduce对输入文件进行二次排序。即第一关键字排序+第二关键字排序。

与传统二次排序不同的是，该项目合并了排序结果：对同一键值的不同值进行按序合并。
如输入

```
Alice 1
Bob 2
Bob 74
Bob 58
Alice 3
ConanC 24
ConanC 233
ConanC 19
```

正常按值排序结果输出为

```
Alice 1
Alice 3
Bob 2
Bob 58
Bob 74
ConanC 19
ConanC 24
ConanC 233
```

合并后输出结果为

```
Alice 1,3
Bob	2,58,74
ConanC 19,24,233
```

## 代码说明

传统的key为

{Alice, 1}

{Alice, 3}

我们首先要重写Mapper把这样的key做一些改变，变为

{[Alice, 1], 1}

{[Alice, 3], 3}

那么我们只需要对新key排序即可，后面的value就可以直接传给reducer做合并了。

当然，这里我们需要自己实现一个比较器，用于二次比较。

但这里产生了一个问题：原来的分区器是将key相同的分成一个区来处理，我们这个是需要新key的**第一个字段**分成同一个区进行处理，所以我们需要自定义一个分区器。

因为我们要按组排序并合并，同理需要自定义分组策略分组给reducer，最后由reducer进行组合即可。

基本每个代码都有写注释，可以直接看注释。

- CombinationKey.java
  - 自定义类，用于存储键值。
- DefinedPartition.java
  - 自定义分区，将新 key 中的第一个字段相同的才放到同一个 reduce 中进行分组合并。
- DefinedGroupSort.java
  - 自定义分组策略
- DefinedComparator.java
  - 自定义比较器
- Sort.java
  - 主类，重要函数SortMapper和SortReducer

```java
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
            logger.info("reduce Input data:{["+key.getFirstKey()+","+ key.getSecondKey()+"],["+sore+"]}");
            logger.info("---------out reduce function flag---------"); 
        } 
    } 
```
