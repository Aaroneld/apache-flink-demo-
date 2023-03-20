package com.merrifield.apacheFlinkDemo;

import com.merrifield.apacheFlinkDemo.model.AccountScopingJSONModel;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;

public class KafkaJob {
    @SneakyThrows
    public static void main(String[] args){
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaRecordDeserializationSchema<AccountScopingJSONModel> inScopeSchema = new AccountScopingJSONModelDeserializationSchema();
        JsonSerializationSchema<AccountScopingJSONModel> inScopeSerializationSchema = new JsonSerializationSchema<>();

        KafkaSource<AccountScopingJSONModel> accountScopingSource =
                KafkaSource.<AccountScopingJSONModel>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics("flink.input.scoping.criteria")
                        .setGroupId("my-group")
                        .setDeserializer(inScopeSchema)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .build();

        DataStream<AccountScopingJSONModel> scopingSourceDataStream = env.fromSource(accountScopingSource, WatermarkStrategy.noWatermarks(), "ScopingSource");

        DataStream<AccountScopingJSONModel> mappedSourceDataStream = scopingSourceDataStream.map((MapFunction<AccountScopingJSONModel, AccountScopingJSONModel>) accountScopingJSONModel -> {
            accountScopingJSONModel.setAccountNumber(99999999);
            return accountScopingJSONModel;
        });

        KafkaSink<AccountScopingJSONModel> sink = KafkaSink.<AccountScopingJSONModel>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(new KafkaRecordSerializationSchemaBuilder<>()
                        .setTopic("in-scope-out-one")
                        .setValueSerializationSchema(inScopeSerializationSchema).build()
                )
                .build();

        mappedSourceDataStream.sinkTo(sink);
        env.execute("mappedSourceDataStream test");
    }

    public static class AccountScopingJSONModelDeserializationSchema implements KafkaRecordDeserializationSchema<AccountScopingJSONModel> {

        @Override
        public TypeInformation<AccountScopingJSONModel> getProducedType() {
            return TypeInformation.of(AccountScopingJSONModel.class);
        }

        @Override
        public void open(DeserializationSchema.InitializationContext context) throws Exception {
            KafkaRecordDeserializationSchema.super.open(context);
        }

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<AccountScopingJSONModel> collector) throws IOException {
            String value = new String(consumerRecord.value());
            // Assume the message value is a JSON object with "name" and "age" properties
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(value);
            AccountScopingJSONModel myObject = new AccountScopingJSONModel();
            myObject.setAccountNumber(jsonNode.get("accountNumber").asInt());
            myObject.setInScope(jsonNode.get("inScope").asBoolean());
            myObject.setEventTimestamp(jsonNode.get("eventTimestamp").asText());
            collector.collect(myObject);
        }
    }
}


