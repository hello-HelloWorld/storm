package com.study.demo1;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/12 13:58
 */

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

//创建bolt--转接头
public class WebLogBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector = null;
    private int num = 0;
    private String str = null;

    // @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            //1.获取传递过来的数据
            str = input.getStringByField("log");
            //2.如果输入的数据不为空，行数++
            if (str != null) {
                num++;
                System.err.println(Thread.currentThread().getName() + "lines  :" + num + "   session_id:" + str.split("\t")[1]);
            }
            //3.回复spout接收成功
            collector.ack(input);
            Thread.sleep(3000);
        } catch (Exception e) {
            //4.应答spout接收失败
            collector.fail(input);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //声明输出字段类型
        declarer.declare(new Fields(""));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
