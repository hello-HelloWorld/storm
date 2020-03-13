package com.study.demo2;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/12 15:05
 */


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

//创建切割单词的bolt--转接头
public class WordCountSplitBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        //1.获取传递过来的一行数据
        String line = input.getString(0);
        //2.分割数据
        String[] words = line.split(" ");
        //3.发射
        for (String word : words) {
            collector.emit(new Values(word, 1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //声明输出的数据类型
        declarer.declare(new Fields("word", "num"));
    }
}
