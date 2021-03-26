package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.writer.FileWriter;
import org.apache.flink.connector.file.sink.writer.FileWriterBucketState;
import org.apache.flink.connector.pulsar.sink.writer.PulsarWriter;
import org.apache.flink.connector.pulsar.sink.writer.PulsarWriterState;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;

import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class PulsarSink<IN> implements Sink<IN, PulsarSinkCommittable, PulsarWriterState, Void> {

    private final String adminUrl;
    private final Optional<String> defaultTopicName;
    private final ClientConfigurationData clientConfigurationData;
    private final Properties properties;
    private final PulsarSerializationSchema<IN> serializationSchema;
    private final MessageRouter messageRouter;
    private final PulsarSinkSemantic semantic;

    public PulsarSink(String adminUrl,
                      Optional<String> defaultTopicName,
                      ClientConfigurationData clientConfigurationData,
                      Properties properties,
                      PulsarSerializationSchema<IN> serializationSchema,
                      MessageRouter messageRouter,
                      PulsarSinkSemantic semantic) {
        this.adminUrl = adminUrl;
        this.defaultTopicName = defaultTopicName;
        this.clientConfigurationData = clientConfigurationData;
        this.properties = properties;
        this.serializationSchema = serializationSchema;
        this.messageRouter = messageRouter;
        this.semantic = semantic;
    }

    @Override
    public SinkWriter createWriter(InitContext initContext, List<PulsarWriterState> states) throws IOException {
        PulsarWriter<IN> writer = new PulsarWriter<>(adminUrl, defaultTopicName,
                clientConfigurationData, properties,
                serializationSchema, messageRouter, semantic);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<Committer<PulsarSinkCommittable>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<PulsarSinkCommittable, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<PulsarSinkCommittable>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<PulsarWriterState>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
