package com.study.demo4;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/13 10:14
 */

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/*
 * 实时计算网站UV去重案例
 *
 * UVBolt1通过fieldGrouping 进行多线程局部汇总，下一级UVSumBolt进行单线程全局汇总去重。按ip地址统计UV数。
 * */

// 创建UVMain驱动
public class UVMain {
    public static void main(String[] args) {
        //1.创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();
        //2.设置spout和bolt
        builder.setSpout("UVSpout", new UVSpout(), 1);
        builder.setBolt("UVBolt1", new UVBolt1(), 4).shuffleGrouping("UVSpout");
        builder.setBolt("UVSumBolt", new UVSumBolt(), 1).shuffleGrouping("UVBolt1");

        //3.配置worker的开启个数
        Config config = new Config();
        config.setNumWorkers(2);

        //4.提交任务
        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("uvtopology", config, builder.createTopology());
        }
    }
}
