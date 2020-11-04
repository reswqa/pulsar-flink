/**
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

package org.apache.flink.connectors.pulsar.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.pulsar.source.util.PulsarAdminUtils;

import com.google.common.collect.Sets;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@PublicEvolving
public class PulsarSourceBuilder<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceBuilder.class);
    // The subscriber specifies the partitions to subscribe to.
    private org.apache.flink.connectors.pulsar.source.PulsarSubscriber subscriber;
    // Users can specify the starting / stopping offset initializer.
    private org.apache.flink.connectors.pulsar.source.StartOffsetInitializer startOffsetInitializer = org.apache.flink.connectors.pulsar.source.StartOffsetInitializer.earliest();
    private StopCondition stopCondition = StopCondition.never();
    // Boundedness
    private Boundedness boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
    private MessageDeserializer<OUT> messageDeserializer;
    // The configurations.
    private Configuration configuration = new Configuration();

    private ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
    private ConsumerConfigurationData<byte[]> consumerConfigurationData = new ConsumerConfigurationData<>();

    PulsarSourceBuilder() {
        consumerConfigurationData.setSubscriptionMode(SubscriptionMode.NonDurable);
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfigurationData.setSubscriptionName("flink-" + UUID.randomUUID());
    }

    public PulsarSourceBuilder<OUT> setTopics(StickyKeyAssigner stickyKeyAssigner, String... topics) {
        TreeSet<String> topicNames = Sets.newTreeSet();
        List<String> collect = Arrays.stream(topics).collect(Collectors.toList());
		for (String topic : collect) {
			topicNames.add(topic);
		}
        consumerConfigurationData.setTopicNames(topicNames);
        return setSubscriber(org.apache.flink.connectors.pulsar.source.PulsarSubscriber.getTopicListSubscriber(stickyKeyAssigner, topics));
    }

    public PulsarSourceBuilder<OUT> setTopicPattern(String namespace, StickyKeyAssigner stickyKeyAssigner, String... topicPatterns) {
        return setSubscriber(org.apache.flink.connectors.pulsar.source.PulsarSubscriber.getTopicPatternSubscriber(namespace, stickyKeyAssigner, topicPatterns));
    }

    public PulsarSourceBuilder<OUT> setSubscriber(org.apache.flink.connectors.pulsar.source.PulsarSubscriber subscriber) {
        checkState(subscriber != null, "topics or topic pattern subscriber already set");
        this.subscriber = subscriber;
        return this;
    }

    public PulsarSourceBuilder<OUT> setTopics(String... topics) {
        return setTopics(StickyKeyAssigner.AUTO, topics);
    }

    public PulsarSourceBuilder<OUT> setTopicPattern(String namespace, String... topicPatterns) {
        return setTopicPattern(namespace, StickyKeyAssigner.AUTO, topicPatterns);
    }

    public PulsarSourceBuilder<OUT> startAt(org.apache.flink.connectors.pulsar.source.StartOffsetInitializer startOffsetInitializer) {
        this.startOffsetInitializer = startOffsetInitializer;
        return this;
    }

    public PulsarSourceBuilder<OUT> stopAt(StopCondition stopCondition) {
        this.boundedness = Boundedness.BOUNDED;
        this.stopCondition = stopCondition;
        return this;
    }

    public <T> PulsarSourceBuilder<T> setDeserializer(MessageDeserializer<T> messageDeserializer) {
        this.messageDeserializer = (MessageDeserializer<OUT>) messageDeserializer;
        return (PulsarSourceBuilder<T>) this;
    }

    public PulsarSourceBuilder<OUT> configure(Consumer<Configuration> configurationConsumer) {
        configurationConsumer.accept(configuration);
        return this;
    }

    public PulsarSourceBuilder<OUT> configurePulsarClient(Consumer<ClientConfigurationData> configurationConsumer) {
        configurationConsumer.accept(clientConfigurationData);
        return this;
    }

    public PulsarSourceBuilder<OUT> configurePulsarConsumer(Consumer<ConsumerConfigurationData> configurationConsumer) {
        configurationConsumer.accept(consumerConfigurationData);
        return this;
    }

    public PulsarSource<OUT> build() {
        sanityCheck();
        return new PulsarSource<>(
                subscriber,
                startOffsetInitializer,
                stopCondition,
                boundedness,
                messageDeserializer,
                configuration,
                clientConfigurationData,
                consumerConfigurationData);
    }

    private <T> boolean maybeOverride(ConfigOption<T> option, T value, boolean override) {
        boolean overridden = false;
        T userValue = configuration.get(option);
        if (userValue != null) {
            if (override) {
                LOG.warn(String.format("Configuration %s is provided but will be overridden from %s to %s",
                        option, userValue, value));
                configuration.set(option, value);
                overridden = true;
            }
        } else {
            configuration.set(option, value);
        }
        return overridden;
    }

    private void sanityCheck() {
        // Check required settings.
        checkNotNull(subscriber, "No subscribe mode is specified, should be one of topics or topic pattern.");
        checkNotNull(messageDeserializer, "Message deserializer is required but not provided.");

        // If the source is bounded, do not run periodic partition discovery.
        if (maybeOverride(
                org.apache.flink.connectors.pulsar.source.PulsarSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS,
                -1L,
                boundedness == Boundedness.BOUNDED)) {
            LOG.warn("{} property is overridden to -1 because the source is bounded.",
                    org.apache.flink.connectors.pulsar.source.PulsarSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS);
        }

        // Check required configs.
        String adminUrl = configuration.getString(org.apache.flink.connectors.pulsar.source.PulsarSourceOptions.ADMIN_URL);
        checkNotNull(adminUrl, org.apache.flink.connectors.pulsar.source.PulsarSourceOptions.ADMIN_URL.key() + " not set.");
        try {
            new ClientBuilderImpl(clientConfigurationData).build();
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Cannot initialize pulsar client", e);
        }
        try {
            PulsarAdminUtils.newAdminFromConf(adminUrl, clientConfigurationData);
        } catch (PulsarClientException e) {
            throw new IllegalStateException("Cannot initialize pulsar admin", e);
        }
    }
}
