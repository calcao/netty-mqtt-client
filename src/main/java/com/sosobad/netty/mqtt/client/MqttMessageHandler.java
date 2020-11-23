package com.sosobad.netty.mqtt.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageBuilders.SubscribeBuilder;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import java.io.UnsupportedEncodingException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author: sosobad
 * @date: 2019/7/11
 */
public class MqttMessageHandler extends SimpleChannelInboundHandler<MqttMessage> {


    private MqttClient mqttClient;

    private MqttProperties mqttProperties;


    public MqttMessageHandler(MqttClient mqttClient) {
        this.mqttClient = mqttClient;
        this.mqttProperties = mqttClient.mqttProperties();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        MqttMessageType messageType = msg.fixedHeader().messageType();


        switch (messageType){
            case PUBREC:
                handlePubRec(ctx, msg);
                break;
            case PUBCOMP:
                System.out.println(">>>" + msg.toString());
                break;
            case PUBREL:
                // 发布释放
                handlePubRel(ctx, msg);
                break;
            case PUBACK:
                // 发布确认
                handlePubAck(ctx, (MqttPubAckMessage)msg);
                break;
            case SUBACK:
                handleSubAck(ctx, (MqttSubAckMessage) msg);
                break;
            case CONNACK:
                handleConnAck(ctx, (MqttConnAckMessage) msg);
                break;
            case PUBLISH:
                handlePublishMessage((MqttPublishMessage)msg);
                break;
            default:
                break;
        }


    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        connect(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }



    private void handleConnAck(ChannelHandlerContext ctx, MqttConnAckMessage mqttConnAckMessage){

        MqttConnectReturnCode code = mqttConnAckMessage.variableHeader()
                .connectReturnCode();

        switch (code){
            case CONNECTION_REFUSED_NOT_AUTHORIZED:
            case CONNECTION_REFUSED_SERVER_UNAVAILABLE:
            case CONNECTION_REFUSED_IDENTIFIER_REJECTED:
            case CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION:
            case CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD:
                System.out.println("connect failed : " + code.name());
                break;
            case CONNECTION_ACCEPTED:
                System.out.println("mqtt broker connected");
                subscribe(ctx);
                break;
            default:
                break;
        }

    }



    private void connect(ChannelHandlerContext ctx){
        MqttConnectMessage connectMessage = MqttMessageBuilders.connect()
                .cleanSession(mqttProperties.isCleanSession())
                .clientId(mqttProperties.getClientId())
                .hasPassword(Objects.nonNull(mqttProperties.getPassword()))
                .hasUser(Objects.nonNull(mqttProperties.getUsername()))
                .keepAlive(mqttProperties.getKeepAliveTimeSeconds())
                .password(mqttProperties.getPassword())
                .protocolVersion(MqttVersion.MQTT_3_1_1)
                .username(mqttProperties.getUsername())
                .willFlag(mqttProperties.isWillFlag())
                .willMessage(mqttProperties.getWillMessage())
                .willQoS(MqttQoS.valueOf(mqttProperties.getWillQos()))
                .willRetain(mqttProperties.isWillRetain())
                .willTopic(mqttProperties.getWillTopic())
                .build();
        ctx.channel().writeAndFlush(connectMessage);
    }


    private void subscribe(ChannelHandlerContext ctx){
        if(Objects.isNull(mqttProperties.getSubscribes())){
            return;
        }
        if(mqttProperties.getSubscribes().isEmpty()){
            return;
        }
        SubscribeBuilder subscribeBuilder = MqttMessageBuilders.subscribe()
                .messageId(1);
        mqttProperties.getSubscribes()
                .forEach(subscribe -> subscribeBuilder.addSubscription(MqttQoS.valueOf(subscribe.qos), subscribe.getTopic()));
        ctx.channel().writeAndFlush(subscribeBuilder.build());
    }


    private void handleSubAck(ChannelHandlerContext ctx, MqttSubAckMessage mqttSubAckMessage){
        System.out.println(mqttSubAckMessage.toString());
    }


    private void handlePubRel(ChannelHandlerContext ctx, MqttMessage mqttMessage){
        System.out.println(">>>" + mqttMessage.toString());

        MqttFixedHeader ack = mqttMessage.fixedHeader();

        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBCOMP, ack.isDup(), ack.qosLevel(), ack.isRetain(), 0x02);

        MqttMessage msg = new MqttMessage(mqttFixedHeader, mqttMessage.variableHeader());

        System.out.println("<<<" + msg.toString());

        ctx.channel().writeAndFlush(msg);
    }


    private void handlePubAck(ChannelHandlerContext ctx, MqttPubAckMessage mqttPubAckMessage){

        System.out.println(">>>" + mqttPubAckMessage.toString());

        MqttFixedHeader ack = mqttPubAckMessage.fixedHeader();

        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, ack.isDup(), ack.qosLevel(), ack.isRetain(), 0x02);

        MqttMessage mqttMessage = new MqttMessage(mqttFixedHeader, mqttPubAckMessage.variableHeader());

        System.out.println("<<<" + mqttMessage.toString());
        ctx.channel().writeAndFlush(mqttMessage);

    }


    private void handlePubRec(ChannelHandlerContext ctx, MqttMessage mqttMessage){
        System.out.println(">>>" + mqttMessage.toString());
        MqttFixedHeader ack = mqttMessage.fixedHeader();
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0x02);
        MqttMessage msg = new MqttMessage(mqttFixedHeader, mqttMessage.variableHeader());
        System.out.println("<<<" + msg.toString());
        ctx.channel().writeAndFlush(msg);
    }


    private void handlePublishMessage(MqttPublishMessage mqttPublishMessage)
            throws UnsupportedEncodingException {
        ByteBuf payload = mqttPublishMessage.payload();
        payload.markReaderIndex();
        byte[] bytes = new byte[payload.readableBytes()];
        payload.readBytes(bytes);
        payload.resetReaderIndex();
        String message = new String(bytes, "utf-8");
        System.out.println(message);
    }


    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        System.out.println("channel inactive!!!");

        ctx.channel().eventLoop()
                .schedule(() -> mqttClient.start(), 1, TimeUnit.SECONDS);
    }
}

