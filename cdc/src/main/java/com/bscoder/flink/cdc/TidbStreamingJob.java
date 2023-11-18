/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xienng.flink.cdc;

import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.TiDBSource;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;

import java.util.HashMap;

import static org.tikv.common.codec.TableCodec.decodeObjects;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class TidbStreamingJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(TidbStreamingJob.class);

    public static void main(String[] args) throws Exception {

//        System.getProperties().put("tikv.grpc.keepalive_timeout", "5000");
//        System.getProperties().put("tikv.grpc.keepalive_time", "5000");
//        System.getProperties().put("tikv.kv_client_concurrency", "1");
//        System.getProperties().put("tikv.kv_client_concurrency", "1");
//        System.getProperties().put("tikv.kv_client_concurrency", "1");
//        System.getProperties().put(ConfigUtils.TIKV_BATCH_SCAN_CONCURRENCY, "1");
//        System.getProperties().put(ConfigUtils.TIKV_BATCH_GET_CONCURRENCY, "1");
//        System.getProperties().put(ConfigUtils.TIKV_INDEX_SCAN_CONCURRENCY, "1");
//        System.getProperties().put(ConfigUtils.TIKV_BATCH_GET_CONCURRENCY, "1");

        //从源码看，在Hashmap里面设置配置是没有用的
        TiConfiguration conf = TDBSourceOptions.getTiConfiguration(
                "10.233.46.8:2379", new HashMap<>());

        TiTableInfo tableInfo = fetchTableInfo(conf);
        LOGGER.info("获取TiTableInfo===================》" + tableInfo);
        SourceFunction<String> tidbSource =
                TiDBSource.<String>builder()
                        // set captured database
                        .database("test")
                        // set captured table
                        .tableName("person")
                        //Tidb不用设置密码。只需要pd addr，flink能连上c
                        .tiConf(conf)
                        //snapshotEventDeserializer历史的数据回调，不会拉增量
                        .snapshotEventDeserializer(
                                new TiKVSnapshotEventDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            Kvrpcpb.KvPair record, Collector<String> out)
                                            throws Exception {

                                        //需要从RowDataTiKVSnapshotEventDeserializationSchema来解析 Kvrpcpb.KvPair的值。RowDataTiKVSnapshotEventDeserializationSchema
                                        //是用来解析Kvrpcpb对象的，从而能生成Sql
                                        Object[] tikvValues =
                                                decodeObjects(
                                                        record.getValue().toByteArray(),
                                                        RowKey.decode(record.getKey().toByteArray()).getHandle(),
                                                        tableInfo);

                                        for (int index = 0; index < tikvValues.length; index++) {
                                            LOGGER.info("获取snapshotEventDeserializer-snapshot===================》" + tikvValues[index]);
                                        }
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }

                                    //changeEventDeserializer是当数据发生变更时的回调。
                                }).changeEventDeserializer(new TiKVChangeEventDeserializationSchema<String>() {
                    @Override
                    public void deserialize(Cdcpb.Event.Row record, Collector<String> out) throws Exception {

                        Object[] tikvValues =
                                decodeObjects(
                                        record.getValue().toByteArray(),
                                        RowKey.decode(record.getKey().toByteArray()).getHandle(),
                                        tableInfo);

                        for (int index = 0; index < tikvValues.length; index++) {
                            LOGGER.info("获取changeEventDeserializer-snapshot===================》" + tikvValues[index]);
                        }

                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;

                    }
                })
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);
        env.addSource(tidbSource).print().setParallelism(1);

        env.execute("Print TiDB Snapshot + Binlog");
    }

    /**
     * 参考的RowDataTiKVSnapshotEventDeserializationSchema实现
     *
     * @param conf
     * @return
     */
    public static TiTableInfo fetchTableInfo(TiConfiguration conf) {
        try (final TiSession session = TiSession.create(conf)) {
            return session.getCatalog().getTable("test", "person");
        } catch (final Exception e) {
            e.printStackTrace();
            throw new FlinkRuntimeException(e);
        }
    }
}
