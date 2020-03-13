package com.study.demo2;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/12 15:11
 */

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

// 创建汇总单词个数的bolt
public class WordCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private Map<String, Integer> map = new HashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        //1.获取传递过来的数据
        String word = input.getString(0);
        Integer num = input.getInteger(1);

        //2，累加单词
        if (map.containsKey(word)) {
            Integer count = map.get(word);
            map.put(word, count + num);
        } else {
            map.put(word, num);
        }
        System.out.println(Thread.currentThread().getId() + "  word:" + word + "  num:" + map.get(word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //不输出
    }
}
