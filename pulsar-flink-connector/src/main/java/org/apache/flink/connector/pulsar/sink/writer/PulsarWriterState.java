package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.pulsar.client.api.transaction.TxnID;

public class PulsarWriterState {
    private final TxnID txnID;

    public PulsarWriterState(TxnID txnID) {
        this.txnID = txnID;
    }

    public TxnID getTxnID() {
        return txnID;
    }


}
