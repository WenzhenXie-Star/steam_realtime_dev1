package com.xwz.retail.v1.realtime.app.retailersv1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.xwz.retail.v1.realtime.function.FilterBloomDeduplicatorFunc;
import com.xwz.retail.v1.realtime.function.MapCheckRedisSensitiveWordsFunc;
import com.xwz.retail.v1.realtime.utils.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @Package com.zsf.retail_v1.realtime.func.DbusBanBlackListUserInfo2Kafka
 * @Author zhao.shuai.fei
 * @Date 2025/5/8 14:38
 * @description: 黑名单封禁 Task-02
 */
public class DbusBanBlackListUserInfo2Kafka {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);

        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "topic_db_sw", "groupId");

        //转json
        SingleOutputStreamOperator<JSONObject> mapJsonStr = kafkaSource.map(JSON::parseObject).uid("to_json_string").name("to_json_string");

        //通过布隆过滤器，去掉重复数据
        SingleOutputStreamOperator<JSONObject> bloomFilterDs = mapJsonStr.keyBy(data -> data.getLong("order_id"))
                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));

        //敏感词检测
        SingleOutputStreamOperator<JSONObject> SensitiveWordsDs = bloomFilterDs.map(new MapCheckRedisSensitiveWordsFunc())
                .uid("MapCheckRedisSensitiveWord")
                .name("MapCheckRedisSensitiveWord");

        //二次检测敏感词
        SingleOutputStreamOperator<JSONObject> secondCheckMap = SensitiveWordsDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                //如果首次检查时判定为非违规 == 0
                if (jsonObject.getIntValue("is_violation") == 0) {
                    // 则对 msg 字段进行进一步的敏感词查找
                    String msg = jsonObject.getString("msg");
                    List<String> msgSen = SensitiveWordHelper.findAll(msg);
                    //更新 is_violation 和 violation_msg 字段
                    if (msgSen.size() > 0) {
                        jsonObject.put("violation_grade", "P1");
                        jsonObject.put("violation_msg", String.join(", ", msgSen));
                    }
                }
                return jsonObject;
            }
        }).uid("second sensitive word check").name("second sensitive word check");

        //写入kafka
        SingleOutputStreamOperator<String> mapped = secondCheckMap.map(s -> s.toJSONString());
        mapped.addSink(KafkaUtil.getKafkaSink("topic_db_blacklist"));

        env.execute();
    }
}
