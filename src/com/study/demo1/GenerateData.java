package com.study.demo1;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/3/12 11:20
 */
/*
 *需求1：将接收到日志的会话id打印在控制台
 * */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

//创建网站访问日志---生成数据
public class GenerateData {
    public static void main(String[] args) {
        File logFile = new File("e:/website.log");
        Random random = new Random();

        //1.网站名臣
        String[] hosts = {"www.baidu.com"};
        //2.会话id
        String[] session_id = {"ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7", "CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678"};
        //3.访问网站时间
        String[] time = {"2017-08-07 08:40:50", "2017-08-07 08:40:51", "2017-08-07 08:40:52", "2017-08-07 08:40:53",
                "2017-08-07 09:40:49", "2017-08-07 10:40:49", "2017-08-07 11:40:49", "2017-08-07 12:40:49"};
        //4.拼接网站访问日志
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < 40; i++) {
            sb.append(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)] + "\n");
        }

        //5.判断日志文件是否存在，不存在则创建
        if (!logFile.exists()) {
            try {
                logFile.createNewFile();
            } catch (IOException e) {
                System.out.println("创建文件失败");
            }
        }
        byte[] bytes = (sb.toString()).getBytes();

        //6.将拼接的日志信息写到日志文件中
        FileOutputStream fs;
        try {
            fs = new FileOutputStream(logFile);
            fs.write(bytes);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
