package com.study.demo1;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/12 14:08
 */

// 需求1：将接收到日志的会话id打印在控制台

/*
 	（1）模拟访问网站的日志信息，包括：网站名称、会话id、访问网站时间等
	（2）将接收到日志的会话id打印到控制台
2）分析
	（1）创建网站访问日志工具类
	（2）在spout中读取日志文件，并一行一行发射出去
	（3）在bolt中将获取到的一行一行数据的会话id获取到，并打印到控制台。
	（4）main方法负责拼接spout和bolt的拓扑。
*/

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class WebLogMain {
    public static void main(String[] args) {
        //1.创建拓扑对象
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        //2.设置spout和bolt
        topologyBuilder.setSpout("weblogspout", new WebLogSpout(), 1);
        topologyBuilder.setBolt("weblogbolt", new WebLogBolt(), 1).shuffleGrouping("weblogspout");

        //3.配置worker开启个数
        Config config = new Config();
        config.setNumWorkers(4);
        if (args.length > 0) {
            try {
                //4.分布式提交
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            //5.本地模式提交
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("weblogtopology", config, topologyBuilder.createTopology());
        }
    }
}
