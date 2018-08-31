package com.pxc.test.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author pengxincheng@ipaynow.cn
 * @Date: 2018/8/30
 * @Time 17:15
 */
public class OrderLyProducer {

    public static void main(String[] args) {

        try {

            DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName1");
            producer.setNamesrvAddr("192.168.11.184:9876");
            producer.setInstanceName("Producer1");
            producer.setVipChannelEnabled(false);
            producer.start();

            String[] tags = new String[]{"create_tag", "pay_tag", "send_tag"};
            for (int orderId = 1; orderId <= 10; orderId++) {
                for (int type = 0; type < 3; type++) {
                    Message message = new Message("orderTopic", tags[type % tags.length], orderId + ":" + type, (orderId + ":" + type).getBytes());

                    SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                            Integer id = (Integer) o;
                            int index = id % list.size();
                            return list.get(index);
                        }
                    }, orderId);
                    System.out.println(sendResult);
                }
            }

            TimeUnit.MILLISECONDS.sleep(1000);

            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
