package com.inori;

import com.inori.constant.MQConstant;
import com.inori.modle.Order;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

;


@SpringBootTest
@Slf4j
class ARocketmqDemoApplicationTests {
    List<Order> orderList = Arrays.asList(
            new Order(1, "KaelvihN", "下单"),
            new Order(1, "KaelvihN", "付款"),
            new Order(1, "KaelvihN", "发货"),
            new Order(2, "Tom", "下单"),
            new Order(2, "Tom", "付款"),
            new Order(2, "Tom", "发货")
    );

    /**
     * 分区有序获取
     */
    @Test
    @SneakyThrows
    void orderConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_consumer_group");
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        consumer.subscribe("orderTopic", "*");
        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                log.info("ThreadId=" + Thread.currentThread().getId() + ">>>" + new String(msg.getBody()));
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        consumer.start();
        System.in.read();

    }

    /**
     * 分区有序发送
     */
    @Test
    @SneakyThrows
    void orderProducer() {
        DefaultMQProducer producer = new DefaultMQProducer("order_producer_group");
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.start();
        for (Order order : orderList) {
            Message message = new Message("orderTopic", order.toString().getBytes());
            producer.send(message, (mqs, msg, arg) -> {
                //通过对id取模使得相同id的订单消息处于同一个队列
                int queueSize = mqs.size();
                Integer id = (Integer) arg;
                int index = id % queueSize;
                return mqs.get(index);
            }, order.getId());
        }
        producer.shutdown();
    }

    /**
     * 接受乱序消息
     */
    @Test
    @SneakyThrows
    void concurrentConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("concurrent_consumer_group");
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        consumer.subscribe("concurrentTopic", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                log.info("ThreadId=" + Thread.currentThread().getId() + ">>>" + new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.in.read();
    }

    /**
     * 发送乱序消息
     */
    @SneakyThrows
    @Test
    void concurrentProducer() {
        DefaultMQProducer producer = new DefaultMQProducer("concurrent_producer_group");
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.start();
        for (int i = 0; i < 5; i++) {
            Message message = new Message("concurrentTopic", ("第" + (i + 1) + "条消息").getBytes());
            producer.send(message);
        }
        producer.shutdown();
        log.info("消息发送已完成");
    }

    /**
     * 发送批量消息
     */
    @SneakyThrows
    @Test
    void batchProducer() {
        DefaultMQProducer producer = new DefaultMQProducer("test_producer_group");
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.start();
        List<Message> msgs = Arrays.asList(
                new Message("batchTopic", "A组消息".getBytes()),
                new Message("batchTopic", "B组消息".getBytes()),
                new Message("batchTopic", "C组消息".getBytes())
        );
        SendResult result = producer.send(msgs);
        log.info(result.toString());
        producer.shutdown();
    }

    /**
     * 接收批量消息
     */
    @Test
    @SneakyThrows
    void batchConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer_group");
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        consumer.subscribe("batchTopic", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                log.info(new String(msg.getBody()));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.in.read();
    }


    /**
     * 接收延迟消息
     */
    @Test
    @SneakyThrows
    void delayConsumer() {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer_group");
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        consumer.subscribe("delayTopic", "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            log.info("接受到消息 = " + new Date());
            log.info(new String(msgs.get(0).getBody()));
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.in.read();
    }

    /**
     * 发布延迟消息
     */
    @SneakyThrows
    @Test
    void delayProducer() {
        //创建生产者，设置生产者组
        DefaultMQProducer producer = new DefaultMQProducer("test_async_producer");
        //设置NameServerAddress
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        //启动
        producer.start();
        //消息发送
        Message message = new Message("delayTopic", "延迟消息".getBytes());
        //延迟等级
        message.setDelayTimeLevel(5);
        producer.send(message);
        //打印时间
        log.info("发送时间 = " + new Date());
        producer.shutdown();
    }

    /**
     * 发布单项消息
     */
    @SneakyThrows
    @Test
    void oneWayProducer() {
        //创建生产者，设置生产者组
        DefaultMQProducer producer = new DefaultMQProducer("test_async_producer");
        //设置NameServerAddress
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.start();
        //发送消息
        Message msg = new Message("testTopic", "单向消息".getBytes());
        producer.sendOneway(msg);
        //关闭生产者
        producer.shutdown();
    }

    /**
     * 发送异步消息
     */
    @SneakyThrows
    @Test
    void asyncProducer() {
        //创建生产者，设置生产者组
        DefaultMQProducer producer = new DefaultMQProducer("test_async_producer");
        //设置NameServerAddress
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.start();
        //发送消息
        Message msg = new Message("testTopic", "异步消息".getBytes());
        producer.send(msg, new SendCallback() {
            //发送成功
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("发送成功");
                log.info("返回结果>>>" + sendResult);
            }

            //发送失败
            @Override
            public void onException(Throwable e) {
                log.error("发送失败");
                log.warn("失败原因" + e);
            }
        });
        log.info("这是一个主线程");
        //挂起JVM
        System.in.read();
        //关闭消费者
        producer.shutdown();
    }

    /**
     * 接收异步消息
     */
    @SneakyThrows
    @Test
    void asyncConsumer() {
        //创建消费者，设置消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_async_consumer");
        //设置NameServerAddress
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        //主题订阅
        consumer.subscribe("testTopic", "*");
        //消息监听,并发消费
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.println(Thread.currentThread().getName() + "==========" + msgs);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        //启动
        consumer.start();
        //阻塞JVM
        System.in.read();
    }

    /**
     * Send Message
     */
    @SneakyThrows
    @Test
    void simpleProducer() {
        //创建Producer(指定组名)
        DefaultMQProducer producer = new DefaultMQProducer("test_producer_group");
        //鏈接NameServer
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.setSendMsgTimeout(60000);
        //启动
        producer.start();
        //创建一个message(Topic,消息体)
        Message msg = new Message("testTopic", "I'm a simple message".getBytes());
        //发送message
        SendResult result = producer.send(msg);
        System.out.println("result = " + result);
        //关闭producer
        producer.shutdown();
    }

    @SneakyThrows
    @Test
    void simpleConsumer() {
        //创建consumer(指定组名)
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer_group");
        //鏈接NameServer
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        //订阅主题(主题名称，订阅表达式=>*表示订阅主题中的所有消息)
        consumer.subscribe("testTopic", "*");
        /**
         * 设置监听器(写业务代码)
         * 异步，一直监听
         * msgs:一个放了消息对象的List
         *context:并发消息的上下文
         */
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            log.info("我是消费者");
            log.info("消息对象=" + new String(msgs.get(0).getBody()));
            log.info("消息的上下文=" + context);
            /**
             * CONSUME_SUCCESS:成功
             * RECONSUME_LATER:失败，消息重新回到队列
             */
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        //启动
        consumer.start();
        //挂起当前jvm，防止主线程结束
        System.in.read();
    }
}
