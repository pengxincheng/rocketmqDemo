package com.pxc.test.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author pengxincheng@ipaynow.cn
 * @Date: 2018/8/24
 * @Time 15:30
 *
 * 两个consumer部署到不同的ip上才可以实现负载均衡消费，一开始在同一机器上启动两个consumer出现了重复消费现象
 */
public class consumer {
    /**
     * 当前例子是PushConsumer用法，使用方式给用户感觉是消息从RocketMQ服务器推到了应用客户端。<br>
     * 但是实际PushConsumer内部是使用长轮询Pull方式从MetaQ服务器拉消息，然后再回调用户Listener方法<br>
     */
    public static void main(String[] args) {
        try {

            /**
             * 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br>
             * 注意：ConsumerGroupName需要由应用来保证唯一
             */
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ConsumerGroupName");
            consumer.setNamesrvAddr("192.168.11.184:9876");
            consumer.setInstanceName("Consumer");

            /**
             * 订阅指定topic下tags分别等于TagA或TagC或TagD
             */
            consumer.subscribe("TopicTest1", "TagA || TagC || TagD");
            /**
             * 订阅指定topic下所有消息<br>
             * 注意：一个consumer对象可以订阅多个topic
             */
          /*  consumer.subscribe("TopicTest2", "*");
            consumer.subscribe("TopicTest3", "*");*/

            //默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs.size());

                MessageExt msg = msgs.get(0);
                try {
                    if (msg.getTopic().equals("TopicTest1")) {
                        // 执行TopicTest1的消费逻辑
                        if (msg.getTags() != null && msg.getTags().equals("TagA")) {
                            System.out.println("TagA" + new String(msg.getBody()));
                        } else if (msg.getTags() != null && msg.getTags().equals("TagC")) {
                            System.out.println("TagC" + new String(msg.getBody()));
                        } else if (msg.getTags() != null && msg.getTags().equals("TagD")) {
                            System.out.println("TagD" + new String(msg.getBody()));
                        }
                    } else if (msg.getTopic().equals("TopicTest2")) {
                        System.out.println("TopicTest2" + new String(msg.getBody()));
                    } else {
                        System.out.println("TopicTest3" + new String(msg.getBody()));
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    e.printStackTrace();
                    if (msg.getReconsumeTimes() == 3) {     //rocketMq默认重试多次   这里设置失败3次不在重试
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } else {
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;            //如果异常返回消费失败 重试
                    }
                }
            });

            /**
             * Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br>
             */
            consumer.start();

            System.out.println("Consumer Started.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
