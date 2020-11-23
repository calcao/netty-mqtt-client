package com.sosobad.netty.mqtt.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: sosobad
 * @date: 2019/7/11
 */
public class MqttClient {

    private AtomicInteger atomicInteger = new AtomicInteger();

    private MqttProperties mqttProperties;


    private Bootstrap bootstrap;


    private EventLoopGroup eventLoopGroup;


    public MqttClient(MqttProperties mqttProperties) {
        this.mqttProperties = mqttProperties;
        bootstrap = new Bootstrap();
        eventLoopGroup = new NioEventLoopGroup(1);
        bootstrap.group(eventLoopGroup);
        bootstrap.handler(new MqttInitializer(this));
        bootstrap.channel(NioSocketChannel.class);
    }


    public MqttProperties mqttProperties() {
        return this.mqttProperties;
    }

    public Channel start()
            throws InterruptedException {

        System.out.println("connecting...");

        ChannelFuture channelFuture = bootstrap.connect(mqttProperties.getHost(), mqttProperties.getPort());

        channelFuture.addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                System.out.println("connected!!!");
            } else {
                System.out.println(future.cause().getMessage());
                System.out.println("reconnect after 30 seconds...");
                EventLoop eventExecutors = future.channel().eventLoop();
                eventExecutors.schedule(() -> start(), 10,
                        TimeUnit.SECONDS);
            }
        });

        channelFuture.sync();

        return channelFuture.channel();
    }


    public void setMessageId(int i) {
        atomicInteger.set(i);
    }


    public void publish(Channel channel, String topic, int qos, byte[] payload) {
        if (channel.isActive()) {
            System.out.println("channel is active");
        } else {
            System.out.println("channel not active");
        }

        atomicInteger.compareAndSet(0xffff, 0);

        int messageId = atomicInteger.addAndGet(1);

        MqttPublishMessage publishMessage = MqttMessageBuilders.publish()
                .messageId(messageId)
                .payload(Unpooled.copiedBuffer(payload))
                .qos(MqttQoS.valueOf(qos))
                .retained(false)
                .topicName(topic)
                .build();

        System.out.println(publishMessage.toString());

        channel.writeAndFlush(publishMessage);
    }


    public static void main(String[] args) throws InterruptedException {
        MqttProperties mqttProperties = new MqttProperties();
        mqttProperties.setHost("localhost");
        MqttClient mqttClient = new MqttClient(mqttProperties);
        Channel channel = mqttClient.start();
        mqttClient.publish(channel, "test", 2, "test qos 2".getBytes());
    }

}
