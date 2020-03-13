package com.study.demo3;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/12 16:52
 */

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

//驱动
public class PVMain {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("pvspount", new PVSpout(), 1);
        builder.setBolt("pvsbolt1", new PVBolt1(), 4).shuffleGrouping("pvspount");//注意此处分配了4个任务，会有4个线程执行
        builder.setBolt("pvsumbolt", new PVSumBolt(), 1).shuffleGrouping("pvsbolt1");//注意此处分配了1个任务，只会有1个线程执行

        Config config = new Config();
        //设置worker的开启个数
        config.setNumWorkers(2);

        //提交模式
        if (args.length > 0) {
            //分布式提交
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            //本地模式提交
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("pvtopology", config, builder.createTopology());
        }
    }
}
