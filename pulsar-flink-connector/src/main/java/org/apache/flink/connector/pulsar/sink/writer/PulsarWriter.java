package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.pulsar.sink.PulsarSinkCommittable;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarClientUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SchemaUtils;
import org.apache.flink.streaming.connectors.pulsar.internal.SourceSinkUtils;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarSinkSemantic;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class PulsarWriter<IN> implements SinkWriter<IN, PulsarSinkCommittable, PulsarWriterState> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarWriter.class);

    private String adminUrl;

    private final ClientConfigurationData clientConfigurationData;

    private final Properties properties;

    protected boolean flushOnCheckpoint;

    protected boolean failOnWrite;

    protected final Map<String, Object> producerConf;

    private final Map<String, String> caseInsensitiveParams;

    private final long transactionTimeout;

    private final long maxBlockTimeMs;

    private int sendTimeOutMs;

    private final PulsarSinkSemantic semantic;

    private final PulsarSerializationSchema<IN> serializationSchema;

    private final MessageRouter messageRouter;

    private transient volatile Throwable failedWrite;

    private transient PulsarAdmin admin;

    private transient BiConsumer<MessageId, Throwable> sendCallback;

    private transient Producer<IN> singleProducer;

    private transient Map<String, Producer<IN>> topic2Producer;

    private ConcurrentHashMap<TxnID, List<MessageId>> tid2MessagesMap;

    private ConcurrentHashMap<TxnID, List<CompletableFuture<MessageId>>> tid2FuturesMap;

    private final boolean forcedTopic;

    private final String defaultTopic;

    private TxnID currentTxnId;

    private Transaction currentTransaction;

    public PulsarWriter(
            String adminUrl,
            Optional<String> defaultTopicName,
            ClientConfigurationData clientConfigurationData,
            Properties properties,
            PulsarSerializationSchema<IN> serializationSchema,
            MessageRouter messageRouter,
            PulsarSinkSemantic semantic) {
        this.adminUrl = checkNotNull(adminUrl);
        this.serializationSchema = serializationSchema;
        this.producerConf =
                SourceSinkUtils.getProducerParams(Maps.fromProperties(properties));
        this.messageRouter = messageRouter;
        this.clientConfigurationData = clientConfigurationData;
        this.properties = properties;
        this.caseInsensitiveParams =
                SourceSinkUtils.toCaceInsensitiveParams(Maps.fromProperties(properties));
        this.transactionTimeout =
                SourceSinkUtils.getTransactionTimeout(caseInsensitiveParams);
        this.maxBlockTimeMs =
                SourceSinkUtils.getMaxBlockTimeMs(caseInsensitiveParams);
        this.sendTimeOutMs =
                SourceSinkUtils.getSendTimeoutMs(caseInsensitiveParams);
        this.flushOnCheckpoint =
                SourceSinkUtils.flushOnCheckpoint(caseInsensitiveParams);
        this.failOnWrite =
                SourceSinkUtils.failOnWrite(caseInsensitiveParams);
        CachedPulsarClient.setCacheSize(SourceSinkUtils.getClientCacheSize(caseInsensitiveParams));

        this.semantic = semantic;

        if (defaultTopicName.isPresent()) {
            this.forcedTopic = true;
            this.defaultTopic = defaultTopicName.get();
        } else {
            this.forcedTopic = false;
            this.defaultTopic = null;
        }

        if (semantic == PulsarSinkSemantic.EXACTLY_ONCE) {
            // in transactional mode, must set producer sendTimeout to 0;
            this.sendTimeOutMs = 0;
            this.tid2MessagesMap = new ConcurrentHashMap<>();
            this.tid2FuturesMap = new ConcurrentHashMap<>();
            clientConfigurationData.setEnableTransaction(true);
        }

        if (this.clientConfigurationData.getServiceUrl() == null) {
            throw new IllegalArgumentException("ServiceUrl must be supplied in the client configuration");
        }

        try {
            admin = PulsarClientUtils.newAdminFromConf(adminUrl, clientConfigurationData);

            serializationSchema.open(new SerializationSchema.InitializationContext() {
                                         @Override
                                         public MetricGroup getMetricGroup() {
                                             return null;
                                         }

                                         @Override
                                         public UserCodeClassLoader getUserCodeClassLoader() {
                                             return this.getUserCodeClassLoader();
                                         }
                                     }
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (forcedTopic) {
            uploadSchema(defaultTopic);
            singleProducer = createProducer(clientConfigurationData, producerConf, defaultTopic,
                    serializationSchema.getSchema());
        } else {
            topic2Producer = new HashMap<>();
        }

    }

    @Override
    public void write(IN value, Context context) throws IOException {
        checkErroneous();
        initializeSendCallback();

        final Optional<String> targetTopic = serializationSchema.getTargetTopic(value);
        String topic = targetTopic.orElse(defaultTopic);
        TypedMessageBuilder<IN> mb = semantic == PulsarSinkSemantic.EXACTLY_ONCE ?
                getProducer(topic).newMessage(currentTransaction) : getProducer(topic).newMessage();

        serializationSchema.serialize(value, mb);

       /* if (flushOnCheckpoint) {
            synchronized (pendingRecordsLock) {
                pendingRecords++;
            }
        }*/

        CompletableFuture<MessageId> messageIdFuture = mb.sendAsync();
        if (currentTransaction != null) {
            // in transactional mode, we must sleep some time because pulsar have some bug can result data disorder.
            // if pulsar-client fix this bug, we can safely remove this.
            Thread.sleep(10);
            List<CompletableFuture<MessageId>> futureList;
            tid2FuturesMap.computeIfAbsent(currentTxnId, key -> new ArrayList<>())
                    .add(messageIdFuture);
            LOG.debug("message {} is invoke in txn {}", value, currentTxnId);
        }
        messageIdFuture.whenComplete(sendCallback);
    }

    public void initializeState(List<PulsarWriterState> states) throws IOException {
        checkNotNull(states, "The retrieved state was null.");

        for (PulsarWriterState state : states) {
            TxnID txnID = state.getTxnID();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Restoring: {}", state);
            }
            //TODO

        }
    }

    @Override
    public List<PulsarSinkCommittable> prepareCommit(boolean flush) throws IOException {
        List<PulsarSinkCommittable> committables = new ArrayList<>();
        switch (semantic) {
            case EXACTLY_ONCE:
            case AT_LEAST_ONCE:
                producerFlush(currentTxnId);
                break;
            case NONE:
                break;
            default:
                throw new UnsupportedOperationException("Not implemented semantic");
        }
        if (currentTxnId != null) {
            LOG.debug("{} preCommit with pending message size {}", currentTxnId, tid2MessagesMap.get(currentTxnId).size());
        } else {
            LOG.debug("in AT_LEAST_ONCE mode, producer was flushed by preCommit");
        }
        checkErroneous();
        committables.add(new PulsarSinkCommittable(tid2MessagesMap.get(currentTxnId), currentTxnId));
        return committables;
    }

    @Override
    public List<PulsarWriterState> snapshotState() throws IOException {
        switch (semantic) {
            case EXACTLY_ONCE:
                LOG.debug("transaction is begining in EXACTLY_ONCE mode");
                Transaction transaction = null;
                try {
                    transaction = createTransaction();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                long txnIdLeastBits = ((TransactionImpl) transaction).getTxnIdLeastBits();
                long txnIdMostBits = ((TransactionImpl) transaction).getTxnIdMostBits();
                TxnID txnID = new TxnID(txnIdMostBits, txnIdLeastBits);
                tid2MessagesMap.computeIfAbsent(txnID, key -> new ArrayList<>());
                tid2FuturesMap.computeIfAbsent(txnID, key -> new ArrayList<>());
                currentTransaction = transaction;
                return Collections.singletonList(new PulsarWriterState(txnID));
            case AT_LEAST_ONCE:
            case NONE:
                return Collections.singletonList(new PulsarWriterState(null));

            default:
                throw new UnsupportedOperationException("Not implemented semantic");
        }
    }

    @Override
    public void close() throws Exception {
        checkErroneous();
        producerClose();
        checkErroneous();
    }

    public void producerFlush(TxnID txnID) throws IOException {
        if (singleProducer != null) {
            singleProducer.flush();
        } else {
            if (topic2Producer != null) {
                for (Producer<?> p : topic2Producer.values()) {
                    p.flush();
                }
            }
        }

        if (txnID != null) {
            // we check the future was completed and add the messageId to list for persistence.
            List<CompletableFuture<MessageId>> futureList = tid2FuturesMap.get(txnID);
            for (CompletableFuture<MessageId> future : futureList) {
                try {
                    MessageId messageId = future.get();
                    tid2MessagesMap.computeIfAbsent(txnID, key -> new ArrayList<>()).add(messageId);
                    LOG.debug("transaction {} add the message {} to messageIdLIst", txnID, messageId);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }

 /*       synchronized (pendingRecordsLock) {
            while (pendingRecords > 0) {
                try {
                    pendingRecordsLock.wait();
                } catch (InterruptedException e) {
                    // this can be interrupted when the Task has been cancelled.
                    // by throwing an exception, we ensure that this checkpoint doesn't get confirmed
                    throw new RuntimeException("Flushing got interrupted while checkpointing", e);
                }
            }
        }*/

        // if the flushed requests has errors, we should propagate it also and fail the checkpoint
        checkErroneous();
    }

    //------------------------------internal method------------------------------

    /**
     * For each checkpoint we create new {@link org.apache.pulsar.client.api.transaction.Transaction} so that new transactions will not clash
     * with transactions created during previous checkpoints.
     */
    private Transaction createTransaction() throws Exception {
        PulsarClientImpl client = CachedPulsarClient.getOrCreate(clientConfigurationData);
        Transaction transaction = client
                .newTransaction()
                .withTransactionTimeout(transactionTimeout, TimeUnit.MILLISECONDS)
                .build()
                .get();
        return transaction;
    }

    protected Producer<IN> getProducer(String topic) {
        if (forcedTopic) {
            return singleProducer;
        }

        if (topic2Producer.containsKey(topic)) {
            return topic2Producer.get(topic);
        } else {
            uploadSchema(topic);
            Producer<IN> p = createProducer(clientConfigurationData, producerConf, topic, serializationSchema.getSchema());
            topic2Producer.put(topic, p);
            return p;
        }
    }

    protected Producer<IN> createProducer(
            ClientConfigurationData clientConf,
            Map<String, Object> producerConf,
            String topic,
            Schema<IN> schema) {

        try {
            ProducerBuilder<IN> builder = CachedPulsarClient
                    .getOrCreate(clientConf)
                    .newProducer(schema)
                    .topic(topic)
                    .sendTimeout(sendTimeOutMs, TimeUnit.MILLISECONDS)
                    .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                    // maximizing the throughput
                    .batchingMaxMessages(5 * 1024 * 1024)
                    .loadConf(producerConf);
            if (messageRouter == null) {
                return builder.create();
            } else {
                return builder.messageRoutingMode(MessageRoutingMode.CustomPartition)
                        .messageRouter(messageRouter)
                        .create();
            }
        } catch (PulsarClientException e) {
            LOG.error("Failed to create producer for topic {}", topic);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            LOG.error("Failed to getOrCreate a PulsarClient");
            throw new RuntimeException(e);
        }
    }

    protected void checkErroneous() {
        Throwable e = failedWrite;
        if (e != null) {
            // prevent double throwing
            failedWrite = null;
            throw new RuntimeException("Failed to send data to Pulsar: " + e.getMessage(), e);
        }
    }

    protected void producerClose() throws Exception {
        producerFlush(currentTxnId);
        if (admin != null) {
            admin.close();
        }
        if (singleProducer != null) {
            singleProducer.close();
        } else {
            if (topic2Producer != null) {
                for (Producer<?> p : topic2Producer.values()) {
                    p.close();
                }
                topic2Producer.clear();
            }
        }
    }

    protected void recoverAndAbort(TxnID txnID) {
            try {
                LOG.debug("transaction {} is recoverAndAbort...", txnID);
                TransactionCoordinatorClientImpl tcClient = CachedPulsarClient.getOrCreate(clientConfigurationData).getTcClient();
                tcClient.abort(txnID, transaction.pendingMessages);
            } catch (ExecutionException executionException) {
                LOG.error("Failed to getOrCreate a PulsarClient");
                throw new RuntimeException(executionException);
            } catch (TransactionCoordinatorClientException.InvalidTxnStatusException statusException) {
                // In some cases, the transaction has been committed or aborted before the recovery,
                // but Flink has not yet sensed it. When flink recover this job, it will commit or
                // abort the transaction again, then Pulsar will throw a duplicate operation error,
                // we catch the error without doing anything to deal with it
                LOG.debug("transaction {} is already aborted...", transaction.transactionalId);
            } catch (TransactionCoordinatorClientException e) {
                throw new RuntimeException(e);
            }
    }

    protected void initializeSendCallback() {
        if (sendCallback != null) {
            return;
        }
        if (failOnWrite) {
            this.sendCallback = (t, u) -> {
                if (failedWrite == null && u == null) {
                    //acknowledgeMessage();
                } else if (failedWrite == null && u != null) {
                    failedWrite = u;
                } else { // failedWrite != null
                    LOG.warn("callback error {}", u);
                    // do nothing and wait next checkForError to throw exception
                }
            };
        } else {
            this.sendCallback = (t, u) -> {
                if (failedWrite == null && u != null) {
                    LOG.error("Error while sending message to Pulsar: {}", ExceptionUtils.stringifyException(u));
                }
                //acknowledgeMessage();
            };
        }
    }

    private void uploadSchema(String topic) {
        SchemaUtils.uploadPulsarSchema(admin, topic, serializationSchema.getSchema().getSchemaInfo());
    }
}
