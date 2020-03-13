package com.study.demo4;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/13 9:59
 */

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

//创建UVBolt1 --转接头
public class UVBolt1 extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        //1.获取传递过来的一行数据
        String lineWords = input.getString(0);
        //2.截取
        String[] words = lineWords.split("\t");
        String ip = words[3];
        //3.发射
        collector.emit(new Values(ip, 1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //设置输出数据类型
        declarer.declare(new Fields("word", "num"));
    }
}
