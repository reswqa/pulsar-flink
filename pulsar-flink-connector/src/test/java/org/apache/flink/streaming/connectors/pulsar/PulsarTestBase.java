/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.pulsar.source.BrokerPartition;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.StartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.StopCondition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.util.PulsarAdminUtils;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.TestLogger;

import io.streamnative.tests.pulsar.service.PulsarService;
import io.streamnative.tests.pulsar.service.PulsarServiceSpec;
import io.streamnative.tests.pulsar.service.testcontainers.PulsarStandaloneContainerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;

/**
 * Start / stop a Pulsar cluster.
 */
@Slf4j
public abstract class PulsarTestBase extends TestLogger {

    protected static PulsarService pulsarService;

    protected static String serviceUrl;

    protected static String adminUrl;

    protected static String zkUrl;

    protected static Configuration configuration = new Configuration();

    protected static ClientConfigurationData clientConfigurationData = new ClientConfigurationData();

    protected static ConsumerConfigurationData<byte[]> consumerConfigurationData = new ConsumerConfigurationData<>();

    protected static PulsarAdmin pulsarAdmin;

    protected static PulsarClient pulsarClient;

    protected static List<String> topics = new ArrayList<>();

    public static String getServiceUrl() {
        return serviceUrl;
    }

    public static String getAdminUrl() {
        return adminUrl;
    }

    @BeforeClass
    public static void prepare() throws Exception {

        adminUrl = System.getenv("PULSAR_ADMIN_URL");
        serviceUrl = System.getenv("PULSAR_SERVICE_URL");
        zkUrl = System.getenv("PULSAR_ZK_URL");

        log.info("-------------------------------------------------------------------------");
        log.info("    Starting PulsarTestBase ");
        log.info("-------------------------------------------------------------------------");

        if (StringUtils.isNotBlank(adminUrl) && StringUtils.isNotBlank(serviceUrl)) {
            pulsarService = mock(PulsarStandaloneContainerService.class);
            log.info("    Use extend Pulsar Service ");
        } else {

            System.setProperty("pulsar.systemtest.image", "streamnative/pulsar:2.7.0-rc-pm-2");
            PulsarServiceSpec spec = PulsarServiceSpec.builder()
                    .clusterName("standalone-" + UUID.randomUUID())
                    .enableContainerLogging(false)
                    .build();

            pulsarService = new PulsarStandaloneContainerService(spec);
            pulsarService.start();
            for (URI uri : pulsarService.getServiceUris()) {
                if (uri != null && uri.getScheme().equals("pulsar")) {
                    serviceUrl = uri.toString();
                } else if (uri != null && !uri.getScheme().equals("pulsar")) {
                    adminUrl = uri.toString();
                }
            }
            zkUrl = ((PulsarStandaloneContainerService) pulsarService).getZkUrl();
            Thread.sleep(80 * 100L);
        }
        clientConfigurationData.setServiceUrl(serviceUrl);
        consumerConfigurationData.setSubscriptionMode(SubscriptionMode.NonDurable);
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfigurationData.setSubscriptionName("flink-" + UUID.randomUUID());

        log.info("-------------------------------------------------------------------------");
        log.info("Successfully started pulsar service");
        log.info("-------------------------------------------------------------------------");
    }

    @AfterClass
    public static void shutDownServices() throws Exception {
        log.info("-------------------------------------------------------------------------");
        log.info("    Shut down PulsarTestBase ");
        log.info("-------------------------------------------------------------------------");

        TestStreamEnvironment.unsetAsContext();
        if (pulsarService != null) {
            pulsarService.stop();
        }
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }

        log.info("-------------------------------------------------------------------------");
        log.info("    PulsarTestBase finished");
        log.info("-------------------------------------------------------------------------");
    }

    protected static Configuration getFlinkConfiguration() {
        Configuration flinkConfig = new Configuration();

        flinkConfig.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "16m");
        flinkConfig.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "my_reporter." +
                ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());
        return flinkConfig;
    }

    public static <T> List<MessageId> sendTypedMessages(
            String topic,
            SchemaType type,
            List<T> messages,
            Optional<Integer> partition) throws PulsarClientException, ExecutionException, InterruptedException {

        return sendTypedMessages(topic, type, messages, partition, null);
    }

    public static <T> Producer<T> getProducer(String topic,
                                              SchemaType type,
                                              Optional<Integer> partition,
                                              Class<T> tClass) throws PulsarClientException, ExecutionException, InterruptedException {
        PulsarClient client = PulsarClient.builder().serviceUrl(getServiceUrl()).build();
        ProducerConfigurationData producerConfigurationData = null;
        if (partition.isPresent()) {
            producerConfigurationData = new ProducerConfigurationData();
            producerConfigurationData.setMessageRoutingMode(MessageRoutingMode.CustomPartition);
            producerConfigurationData.setTopicName(topic);
            producerConfigurationData.setCustomMessageRouter(new MessageRouter() {
                @Override
                public int choosePartition(Message<?> msg) {
                    return partition.get();
                }
            });
        }

        Producer producer = null;

        switch (type) {
            case BOOLEAN:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.BOOL, null).get() :
                        (Producer<T>) client.newProducer(Schema.BOOL).topic(topic).create();
                break;
            case BYTES:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.BYTES, null).get() :
                        (Producer<T>) client.newProducer(Schema.BYTES).topic(topic).create();
                break;
            case LOCAL_DATE:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.LOCAL_DATE, null).get() :
                        (Producer<T>) client.newProducer(Schema.LOCAL_DATE).topic(topic).create();
                break;
            case DATE:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.DATE, null).get() :
                        (Producer<T>) client.newProducer(Schema.DATE).topic(topic).create();
                break;
            case STRING:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.STRING, null).get() :
                        (Producer<T>) client.newProducer(Schema.STRING).topic(topic).create();
                break;
            case TIMESTAMP:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.TIMESTAMP, null).get() :
                        (Producer<T>) client.newProducer(Schema.TIMESTAMP).topic(topic).create();
                break;
            case LOCAL_DATE_TIME:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.LOCAL_DATE_TIME, null).get() :
                        (Producer<T>) client.newProducer(Schema.LOCAL_DATE_TIME).topic(topic).create();
                break;
            case INT8:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.INT8, null).get() :
                        (Producer<T>) client.newProducer(Schema.INT8).topic(topic).create();
                break;
            case DOUBLE:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.DOUBLE, null).get() :
                        (Producer<T>) client.newProducer(Schema.DOUBLE).topic(topic).create();
                break;
            case FLOAT:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.FLOAT, null).get() :
                        (Producer<T>) client.newProducer(Schema.FLOAT).topic(topic).create();
                break;
            case INT32:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.INT32, null).get() :
                        (Producer<T>) client.newProducer(Schema.INT32).topic(topic).create();
                break;
            case INT16:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.INT16, null).get() :
                        (Producer<T>) client.newProducer(Schema.INT16).topic(topic).create();
                break;
            case INT64:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.INT64, null).get() :
                        (Producer<T>) client.newProducer(Schema.INT64).topic(topic).create();
                break;
            case AVRO:
                SchemaDefinition<Object> schemaDefinition =
                        SchemaDefinition.builder().withPojo(tClass).withJSR310ConversionEnabled(true).build();
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.AVRO(schemaDefinition), null).get() :
                        (Producer<T>) client.newProducer(Schema.AVRO(schemaDefinition)).topic(topic).create();
                break;
            case JSON:
                producer = partition.isPresent() ?
                        ((PulsarClientImpl) client).createProducerAsync(producerConfigurationData, Schema.JSON(tClass), null).get() :
                        (Producer<T>) client.newProducer(Schema.JSON(tClass)).topic(topic).create();
                break;

            default:
                throw new NotImplementedException("Unsupported type " + type);
        }
        return producer;
    }

    public static <T> List<MessageId> sendTypedMessages(
            String topic,
            SchemaType type,
            List<T> messages,
            Optional<Integer> partition,
            Class<T> tClass) throws PulsarClientException, ExecutionException, InterruptedException {

        Producer<T> producer = getProducer(topic, type, partition, tClass);
        List<MessageId> mids = new ArrayList<>();

        for (T message : messages) {
            MessageId mid = sendMessageInternal(producer, message, null, null, null, null);
            log.info("Sent {} of mid: {}", message.toString(), mid.toString());
            mids.add(mid);
        }

        return mids;
    }

    public static <T> List<MessageId> sendTypedMessagesWithMetadata(String topic,
                                                                    SchemaType type,
                                                                    List<T> messages,
                                                                    Optional<Integer> partition,
                                                                    Class<T> tClass,
                                                                    List<Long> eventTimes,
                                                                    List<Long> sequenceIds,
                                                                    List<Map<String, String>> properties,
                                                                    List<String> keys
    ) throws PulsarClientException, ExecutionException, InterruptedException {
        Producer<T> producer = getProducer(topic, type, partition, tClass);
        List<MessageId> mids = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            MessageId mid = sendMessageInternal(producer, messages.get(i),
                    eventTimes.get(i), sequenceIds.get(i), properties.get(i), keys.get(i));
            log.info("Sent {} of mid: {}", messages.get(i).toString(), mid.toString());
            mids.add(mid);
        }
        return mids;
    }

    private static <T> MessageId sendMessageInternal(Producer<T> producer,
                                                     T message,
                                                     Long eventTime,
                                                     Long sequenceId,
                                                     Map<String, String> properties,
                                                     String key
    ) throws PulsarClientException {
        TypedMessageBuilder<T> mb = producer.newMessage().value(message);
        if (eventTime != null) {
            mb = mb.eventTime(eventTime);
        }
        if (sequenceId != null) {
            mb = mb.sequenceId(sequenceId);
        }
        if (properties != null) {
            mb = mb.properties(properties);
        }
        if (key != null) {
            mb = mb.key(key);
        }
        return mb.send();
    }

    // --------------------- public client related helpers ------------------

    public static PulsarAdmin getPulsarAdmin() {
        try {
            return PulsarAdminUtils.newAdminFromConf(adminUrl, clientConfigurationData);
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Cannot initialize pulsar admin", e);
        }
    }

    public static PulsarClient getPulsarClient() {
        try {
            return new ClientBuilderImpl(clientConfigurationData).build();
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Cannot initialize pulsar client", e);
        }
    }

    // ------------------- topic information helpers -------------------

    public static void createTestTopic(String topic, int numberOfPartitions) throws Exception {
        if (pulsarAdmin == null) {
            pulsarAdmin = getPulsarAdmin();
        }
        if (numberOfPartitions == 0) {
            pulsarAdmin.topics().createNonPartitionedTopic(topic);
        } else {
            pulsarAdmin.topics().createPartitionedTopic(topic, numberOfPartitions);
        }
    }

    public static List<BrokerPartition> getPartitionsForTopic(String topic) throws Exception {
        return pulsarClient.getPartitionsForTopic(topic).get()
                .stream()
                .map(pi -> new BrokerPartition(new TopicRange(pi, BrokerPartition.FULL_RANGE)))
                .collect(Collectors.toList());
    }

    public static Map<Integer, Map<String, PulsarPartitionSplit>> getSplitsByOwners(
            final Collection<String> topics,
            final int numSubtasks) throws Exception {
        final Map<Integer, Map<String, PulsarPartitionSplit>> splitsByOwners = new HashMap<>();
        for (String topic : topics) {
            getPartitionsForTopic(topic).forEach(partition -> {
                int ownerReader = ((partition.hashCode() * 31) & 0x7FFFFFFF) % numSubtasks;
                PulsarPartitionSplit split = new PulsarPartitionSplit(
                        partition, StartOffsetInitializer.earliest(), StopCondition.stopAfterLast());
                splitsByOwners
                        .computeIfAbsent(ownerReader, r -> new HashMap<>())
                        .put(partition.toString(), split);
            });
        }
        return splitsByOwners;
    }

    public static String newTopic() {
        final String topic = TopicName.get("topic" + RandomStringUtils.randomNumeric(8)).toString();
        topics.add(topic);
        return topic;
    }

    public static Configuration getConsumerConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(PulsarSourceOptions.ADMIN_URL, adminUrl);
        return configuration;
    }
}
