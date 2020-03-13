package com.study.demo3;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/12 16:33
 */

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

//创建PVSumBolt
public class PVSumBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private Map<Long, Long> counts = new HashMap<>();


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple input) {
        Long threadId = input.getLong(0);
        Long pv = input.getLong(1);
        counts.put(threadId, pv);

        long word_sum = 0;
        Iterator<Long> iterable = counts.values().iterator();

        while (iterable.hasNext()) {
            word_sum += iterable.next();
        }

        System.out.println("线程：" + Thread.currentThread().getId() + "总数为：" + word_sum);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//不输出
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
