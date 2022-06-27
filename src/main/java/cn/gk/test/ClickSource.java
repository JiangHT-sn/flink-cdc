package cn.gk.test;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author: HT
 * @Date: 2022/3/25
 * @Description:
 */
public class ClickSource implements SourceFunction<String> {
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Random random = new Random(); // 在指定的数据集中随机选取数据

        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        while (running) {
            for (String user : users) {
                ctx.collect(user);
            }
            // 隔 1 秒生成一个点击事件，方便观测
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}