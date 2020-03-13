package com.study.demo3;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/12 16:23
 */

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

// 创建数据处理pvbolt1
public class PVBolt1 implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private long pv = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /*
     * 注意：是由多个线程执行该方法，pv变量在每个线程中分别独立，累加
     * */
    @Override
    public void execute(Tuple input) {
        //获取传递过来的数据
        String line = input.getString(0);
        //截取出session_id
        String session_id = line.split("\t")[1];
        //根据会话id不同统计pv数
        if (session_id != null) {
            pv++;
        }

        //发射
        collector.emit(new Values(Thread.currentThread().getId(), pv));
        System.out.println("threadid:" + Thread.currentThread().getId() + "  pv:" + pv);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //输出数据类型
        declarer.declare(new Fields("threadId", "pv"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
