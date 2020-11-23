package com.sosobad.netty.mqtt.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * @author: sosobad
 * @date: 2019/7/11
 */
public class MqttPingEventHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        boolean mqttMessageFlag = msg instanceof MqttMessage;
        if (!mqttMessageFlag) {
            ctx.fireChannelRead(msg);
            return;
        }
        MqttMessage mqttMessage = (MqttMessage) msg;
        MqttMessageType messageType = mqttMessage.fixedHeader().messageType();
        if (MqttMessageType.PINGREQ.equals(messageType)) {
            ping(ctx);
        } else if (MqttMessageType.PINGRESP.equals(messageType)) {
            ping(ctx);
        } else {
            ctx.fireChannelRead(msg);
        }
    }


    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
        boolean idleEventFlag = evt instanceof IdleStateEvent;
        if(!idleEventFlag){
            return;
        }
        IdleStateEvent event = (IdleStateEvent) evt;

        if(IdleStateEvent.WRITER_IDLE_STATE_EVENT.state().equals(event.state())){
            pang(ctx);
        }

        if(IdleStateEvent.READER_IDLE_STATE_EVENT.state().equals(event.state())){

        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }


    private void ping(ChannelHandlerContext ctx) {
        ctx.channel().writeAndFlush(createPingMessage(MqttMessageType.PINGREQ));
    }

    private void pang(ChannelHandlerContext ctx) {
        ctx.channel().writeAndFlush(createPingMessage(MqttMessageType.PINGRESP));
    }


    private MqttMessage createPingMessage(MqttMessageType mqttMessageType){
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(mqttMessageType, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessage mqttMessage = new MqttMessage(mqttFixedHeader);
        return mqttMessage;
    }
}

