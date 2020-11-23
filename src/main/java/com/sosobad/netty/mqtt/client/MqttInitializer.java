package com.sosobad.netty.mqtt.client;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * @author: sosobad
 * @date: 2019/7/11
 */
public class MqttInitializer extends ChannelInitializer<SocketChannel> {


    private MqttClient mqttClient;

    public MqttInitializer(MqttClient mqttClient) {
        this.mqttClient = mqttClient;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("MqttDecoder", new MqttDecoder());
        pipeline.addLast("MqttEncoder", MqttEncoder.INSTANCE);
        pipeline.addLast("IdleStateHandler", new IdleStateHandler(0,mqttClient.mqttProperties().getKeepAliveTimeSeconds(), 0));
        pipeline.addLast("PingHandler", new MqttPingEventHandler());
        pipeline.addLast("MqttMessageHandler", new MqttMessageHandler(mqttClient));
    }
}
