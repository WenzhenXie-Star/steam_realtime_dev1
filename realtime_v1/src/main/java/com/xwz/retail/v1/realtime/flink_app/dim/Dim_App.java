package com.xwz.retail.v1.realtime.flink_app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.xwz.retail.v1.realtime.bean.TableProcessDim;
import com.xwz.retail.v1.realtime.constant.Constant;
import com.xwz.retail.v1.realtime.utils.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @Package com.xwz.retail.v1.realtime.flink_app.dim.Dim_App
 * @Author  Wenzhen.Xie
 * @Date  2025/4/9 22:17
 * @description: 
*/

public class Dim_App {
    public static void main(String[] args) throws Exception {
        //TOP1.基本环境
        //1.1指定流处理环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置行并度
        env.setParallelism(1);

        //TOP2.检查点设置
        //2.1开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3设置job取消检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5设置重启策略
        //(固定延时重启)
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //(故障率重启:30天3次机会3秒一次)
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6设置状态后端以及检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck");
        //2.7设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","root");

        //TOP3从kafka的topic_db主题中读取业务数据
        //3.1声明消费的主题以及消费者组
        String topic="topic-db";
        String groupId="dim_app_group";
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
                .setTopics(topic)
                .setGroupId(groupId)
//                .setStartingOffsets(OffsetsInitializer.committedOffsets())从消费组提交的位点开始消费，不指定位点重置策略
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))从消费组提交的位点开始消费，如果提交位点不存在则使用最早位点
//                .setStartingOffsets(OffsetsInitializer.timestamp(1657256176000L))从时间戳大于等于指定时间戳的数据开始消费
//                .setStartingOffsets(OffsetsInitializer.earliest())从最早点位开始消费latest
                .setStartingOffsets(OffsetsInitializer.latest())//从最末点位开始消费
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if (bytes!=null){
                                    return new String(bytes);
                                }
                                return null;
                            }
                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }
                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDs = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
//        kafkaStrDs.print();
        //TOP4.对业务流中数据类型进行转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDs.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        com.alibaba.fastjson.JSONObject jsonObj = JSON.parseObject(s);
                        String db = jsonObj.getJSONObject("source").getString("db");
                        String type = jsonObj.getString("op");
                        String data = jsonObj.getString("after");
                        if ("db_gmall".equals(db)
                                && ("c".equals(type)
                                || "u".equals(type)
                                || "d".equals(type)
                                || "r".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            collector.collect(jsonObj);
                        }
                    }
                }
        );
//        jsonObjDS.print();

        //TOP5.使用FlinkCDC读取配置表里的配置信息
        //5.1创建MySQLSource对象
        Properties pro = new Properties();
        Properties props=new Properties();
        props.setProperty("useSSL","false");
        props.setProperty("allowPublicKeyRetrieval","true");
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("realtime_v1")
                .tableList("realtime_v1.table_process_dim")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(pro)
                .build();
        //5.2 读取数据封装为流
        DataStreamSource<String> mysqlStrDs = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
//        mysqlStrDs.print();
        //TOP6.对配置流中的数据类型进行转换 jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDs.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String s) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(s);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if("d".equals(op)){
                            //对配置表进行了一次删除操作   从before属性中获取删除前的配置信息
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        }else{
                            //对配置表进行了读取、添加、修改操作   从after属性中获取最新的配置信息
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

        //TOP7.根据配置表中的配置信息给到HBase中执行建表或者删除表操作
        tpDS=tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hbaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn= HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //操作类型
                        String op = tp.getOp();
                        //Hbase中的表名
                        String sinkTable = tp.getSinkTable();
                        //Hbase中的表的列族
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if ("d".equals(op)){
                            //对维度配置表进行删除，我们要删除Hbase对应的表
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){
                            //对维度配置表进行添加或读取，我们要对Hbase进行建表
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else {
                            //对维度配置表进行修改，我们要对Hbase的表先删除在创建
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }

                        return tp;
                    }
                }
        ).setParallelism(1);
//        tpDS.print();

        //TOP8.将配置表中的配置信息进行广播---broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class,TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //TOP9.将主流业务数据和广播流配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        //TOP10.处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>() {
                    private Map<String,TableProcessDim> configMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Class.forName("com.mysql.cj.jdbc.Driver");

                        java.sql.Connection conn= DriverManager.getConnection(Constant.MYSQL_URL,Constant.MYSQL_USER_NAME,Constant.MYSQL_PASSWORD);

                        String sql="select * from realtime_v1.table_process_dim";

                        PreparedStatement ps = conn.prepareStatement(sql);

                        ResultSet rs = ps.executeQuery();

                        ResultSetMetaData metaData = rs.getMetaData();

                        while (rs.next()){
                            JSONObject jsonObj = new JSONObject();
                            for (int i=1 ; i<=metaData.getColumnCount(); i++){
                                String columnName = metaData.getColumnName(i);
                                Object objectValue = rs.getObject(i);
                                jsonObj.put(columnName,objectValue);
                            }
                            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
                            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
                        }

                        rs.close();
                        ps.close();
                        conn.close();
                    }

                    //处理主流业务数据 根据维度表名到广播状态中读取配置信息，判断是否为维度
                    @Override
                    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {

                        String table = jsonObject.getJSONObject("source").getString("table");
                        String op = jsonObject.getString("op");

                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

                        TableProcessDim tableProcessDim = null;


                        if ((tableProcessDim = broadcastState.get(table)) != null
                                || (tableProcessDim = configMap.get(table))!=null){
                            JSONObject dataJsonObj = new JSONObject();
                            if (op.equals("d")){
                                dataJsonObj = jsonObject.getJSONObject("before");
                            }else {
                                dataJsonObj = jsonObject.getJSONObject("after");
                            }
                            deleteNeedColumns(dataJsonObj,tableProcessDim.getSinkColumns());
                            dataJsonObj.put("type",op);
                            collector.collect(Tuple2.of(dataJsonObj,tableProcessDim));
                        }
                    }

                    //处理广播流配置信息 将配置数据放到广播状态中 k:维度表名 v:配置对象
                    @Override
                    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context context, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
                        //获取对配置表进行的操作类型
                        String op = tableProcessDim.getOp();
                        //获取广播状态
                        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        //获取维度表名称
                        String sourceTable = tableProcessDim.getSourceTable();
                        if ("d".equals(op)){
                            broadcastState.remove(sourceTable);
                        }else {
                            broadcastState.put(sourceTable,tableProcessDim);
                        }
                    }
                }
        );

        //TOP11.将位数数据同步到HBase表中
        dimDS.print();
//({"name":"图书、音像、电子书刊","id":1,"type":"u"},TableProcessDim(sourceTable=base_category1, sinkTable=dim_base_category1, sinkColumns=id,name, sinkFamily=info, sinkRowKey=id, op=r))
        dimDS.addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn=HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(hbaseConn);
            }

            @Override
            public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
                JSONObject jsonObject = value.f0;
                TableProcessDim tableProcessDim = value.f1;
                String type = jsonObject.getString("type");
                jsonObject.remove("type");

                String sinkTable = tableProcessDim.getSinkTable();
                String rowkey = jsonObject.getString(tableProcessDim.getSinkRowKey());

                if ("d".equals(type)){
                    HBaseUtil.delRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowkey);
                }else {
                    String sinkFamily = tableProcessDim.getSinkFamily();
                    HBaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowkey,sinkFamily,jsonObject);
                }
            }
        });


        env.execute("Dim_App");
    }

    private static void deleteNeedColumns(JSONObject dataJsonObj,String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
    }
}
























