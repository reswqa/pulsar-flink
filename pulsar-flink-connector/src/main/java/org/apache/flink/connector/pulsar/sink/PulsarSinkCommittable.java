package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.annotation.Internal;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;

import java.io.Serializable;
import java.util.List;

@Internal
public class PulsarSinkCommittable implements Serializable {
    private final List<MessageId> PendingMessageIds;

    private final TxnID txnID;

    public PulsarSinkCommittable(List<MessageId> pendingMessageIds, TxnID txnID) {
        this.PendingMessageIds = pendingMessageIds;
        this.txnID = txnID;
    }

    public List<MessageId> getPendingMessageIds() {
        return PendingMessageIds;
    }

    public TxnID getTxnID() {
        return txnID;
    }
}
