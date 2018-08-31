package com.pxc.test.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author pengxincheng@ipaynow.cn
 * @Date: 2018/8/30
 * @Time 17:36
 */
public class OrderlyConsumer {
    public static void main(String[] args) {
        try {

            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ConsumerGroupName1");
            consumer.setNamesrvAddr("192.168.11.184:9876");
            consumer.setInstanceName("Consumer1");

            consumer.subscribe("orderTopic", "*");

            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                    try {
                        Thread.sleep(1000);
                        System.out.println(new String(list.get(0).getBody(),"utf-8"));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });

            consumer.start();

            System.out.println("Consumer Started.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
