package com.study.demo2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/12 15:18
 */
/*      1）需求
        实时统计发射到Storm框架中单词的总数。
        2）分析
        设计一个topology，来实现对文档里面的单词出现的频率进行统计。
        整个topology分为三个部分：
        （1）WordCountSpout：数据源，在已知的英文句子中，随机发送一条句子出去。
        （2）WordCountSplitBolt：负责将单行文本记录（句子）切分成单词
        （3）WordCountBolt：负责对单词的频率进行累加*/

// 创建程序的拓扑main
public class WordCountMain {
    public static void main(String[] args) {
        //1.准备一个topologyBuilder
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("wordcount", new WordCountSpout(), 1);
        topologyBuilder.setBolt("wordcountsplitbolt", new WordCountSplitBolt(), 2).shuffleGrouping("wordcount");
        topologyBuilder.setBolt("wordcountbolt", new WordCountBolt(), 4).fieldsGrouping("wordcountsplitbolt", new Fields("word"));

        //2.配置worker的开启个数
        Config config = new Config();
        config.setNumWorkers(2);

        //3.提交任务
        if (args.length > 0) {
            try {
                //4.分布式提交
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            //5.本地提交
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordcounttopology", config, topologyBuilder.createTopology());
        }
    }
}
