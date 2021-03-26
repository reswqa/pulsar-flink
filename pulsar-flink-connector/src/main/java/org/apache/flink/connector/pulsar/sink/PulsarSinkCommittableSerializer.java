package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PulsarSinkCommittableSerializer implements SimpleVersionedSerializer<PulsarSinkCommittable> {
    private static final int MAGIC_NUMBER = 0x1e765c80;

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(PulsarSinkCommittable committable) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(committable, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public PulsarSinkCommittable deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        switch (version) {
            case 1:
                validateMagicNumber(in);
                return deserializeV1(in);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private void serializeV1(
            PulsarSinkCommittable record,
            DataOutputView target) throws IOException {
        if (record.getTxnID() == null) {
            target.writeBoolean(false);
        } else {
            target.writeBoolean(true);
            target.writeLong(record.getTxnID().getMostSigBits());
            target.writeLong(record.getTxnID().getLeastSigBits());
            int size = record.getPendingMessageIds().size();
            target.writeInt(size);
            for (MessageId messageId : record.getPendingMessageIds()) {
                byte[] messageData = messageId.toByteArray();
                target.writeInt(messageData.length);
                target.write(messageData);
            }
        }
    }

    private PulsarSinkCommittable deserializeV1(DataInputView dataInputView) throws IOException {
        TxnID transactionalId = null;
        List<MessageId> pendingMessages = new ArrayList<>();
        if (dataInputView.readBoolean()) {
            long mostSigBits = dataInputView.readLong();
            long leastSigBits = dataInputView.readLong();
            transactionalId = new TxnID(mostSigBits, leastSigBits);
            int size = dataInputView.readInt();
            for (int i = 0; i < size; i++) {
                int length = dataInputView.readInt();
                byte[] messageData = new byte[length];
                dataInputView.read(messageData);
                pendingMessages.add(MessageId.fromByteArray(messageData));
            }
        }
        return new PulsarSinkCommittable(pendingMessages, transactionalId);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(String.format(
                    "Corrupt data: Unexpected magic number %08X",
                    magicNumber));
        }
    }
}
