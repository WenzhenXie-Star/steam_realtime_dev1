package com.xwz.retail.v1.realtime.flink_app.dwd.realtime_dwd_base_log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.xwz.retail.v1.realtime.base.BaseApp;
import com.xwz.retail.v1.realtime.constant.Constant;
import com.xwz.retail.v1.realtime.utils.DateFormatUtil;
import com.xwz.retail.v1.realtime.utils.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Package com.xwz.retail.v1.realtime.flink_app.dwd.realtime_dwd_base_log.Dwd_Base_Log
 * @Author  Wenzhen.Xie
 * @Date  2025/4/14 10:38
 * @description: 
*/

public class Dwd_Base_Log extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dwd_Base_Log().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void start(int port, int parallelism, String ckAndGroupId, String topic) throws Exception {
        super.start(port, parallelism, ckAndGroupId, topic);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaStrDS.process(

                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(s);
                            collector.collect(jsonObj);
                        } catch (Exception e) {
                            context.output(dirtyTag, s);
                        }
                    }
                }
        );
//        jsonObjDs.print("标准的JSON:");
        SideOutputDataStream<String> dirtyDS = jsonObjDs.getSideOutput(dirtyTag);
//        dirtyDS.print("脏数据:");

//        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
//                .setBootstrapServers(Constant.KAFKA_BROKERS)
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic("dirty_data")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build()
//                )
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix("dwd_base_log")
//                .build();
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);


        KeyedStream<JSONObject, String> keyedDS = jsonObjDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        final SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateStare;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<String>("lastVisitDateStare", String.class);
                        lastVisitDateStare = getRuntimeContext().getState(stringValueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        String lastVisitDate = lastVisitDateStare.value();
                        Long ts = jsonObject.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateStare.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateStare.update(yesterDay);
                            }
                        }
                        return jsonObject;
                    }
                }
        );
        
        //错误
        OutputTag<String> errTag = new OutputTag<String>("errTag"){};
        //启动
        OutputTag<String> startTag = new OutputTag<String>("startTag"){};
        //曝光
        OutputTag<String> displayTag = new OutputTag<String>("displayTag"){};
        //动作
        OutputTag<String> actionTag = new OutputTag<String>("actionTag"){};
        //主流是页面
        final SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        JSONObject errjsonObj = jsonObject.getJSONObject("err");
                        if (errjsonObj != null) {
                            context.output(errTag, jsonObject.toJSONString());
                            jsonObject.remove("err");
                        }

                        JSONObject startJsonObj = jsonObject.getJSONObject("start");
                        if (startJsonObj != null) {
                            context.output(startTag, jsonObject.toJSONString());
                        } else {
                            JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                            Long ts = jsonObject.getLong("ts");
                            JSONArray displaysArr = jsonObject.getJSONArray("displays");
                            if (displaysArr != null && displaysArr.size() > 0) {
                                for (int i = 0; i < displaysArr.size(); i++) {
                                    JSONObject displaysJSONObj = displaysArr.getJSONObject(i);
                                    JSONObject newdisplaysJSONObj = new JSONObject();
                                    newdisplaysJSONObj.put("common", commonJsonObj);
                                    newdisplaysJSONObj.put("page", pageJsonObj);
                                    newdisplaysJSONObj.put("display", displaysJSONObj);
                                    newdisplaysJSONObj.put("ts", ts);
                                    context.output(displayTag, newdisplaysJSONObj.toJSONString());
                                }
                                jsonObject.remove("displays");
                            }

                            JSONArray actionsArr = jsonObject.getJSONArray("actions");
                            if (actionsArr != null && actionsArr.size() > 0) {
                                for (int i = 0; i < actionsArr.size(); i++) {
                                    JSONObject actionjsonObj = actionsArr.getJSONObject(i);
                                    JSONObject newActionjsonObj = new JSONObject();
                                    newActionjsonObj.put("common", commonJsonObj);
                                    newActionjsonObj.put("page", pageJsonObj);
                                    newActionjsonObj.put("actions", actionjsonObj);
                                    context.output(actionTag, newActionjsonObj.toJSONString());
                                }
                                jsonObject.remove("actions");
                            }
                            collector.collect(jsonObject.toJSONString());
                        }

                    }
                }
        );
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.print("页面:");
        errDS.print("错误:");
        startDS.print("启动:");
        displayDS.print("曝光:");
        actionDS.print("动作");

        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));



    }
}








