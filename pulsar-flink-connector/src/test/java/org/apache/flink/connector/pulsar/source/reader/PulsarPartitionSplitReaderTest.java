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

package org.apache.flink.connector.pulsar.source.reader;

import org.apache.flink.common.IntegerDeserializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.pulsar.source.AbstractPartition;
import org.apache.flink.connector.pulsar.source.BrokerPartition;
import org.apache.flink.connector.pulsar.source.MessageDeserializer;
import org.apache.flink.connector.pulsar.source.StartOffsetInitializer;
import org.apache.flink.connector.pulsar.source.StopCondition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.streaming.connectors.pulsar.PulsarTestBase;
import org.apache.flink.streaming.connectors.pulsar.SchemaData;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicRange;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for {@link PulsarPartitionSplitReader}.
 */
public class PulsarPartitionSplitReaderTest extends PulsarTestBase {
	private static final int NUM_SUBTASKS = 2;
	private static final String TOPIC1 = "topic1";
	private static final String TOPIC2 = "topic2";
	private static final int NUM_PARTITIONS = 2;
	private static Map<Integer, Map<String, PulsarPartitionSplit>> splitsByOwners;

	private static Map<String, List<MessageId>> mid2topic1 = new HashMap<>();
	private static Map<String, List<MessageId>> mid2topic2 = new HashMap<>();

	@BeforeClass
	public static void setup() throws Throwable {
		pulsarAdmin = getPulsarAdmin();
		pulsarClient = getPulsarClient();
		createTestTopic(TOPIC1, NUM_PARTITIONS);
		createTestTopic(TOPIC2, NUM_PARTITIONS);
		for(int i = 0; i < NUM_PARTITIONS; i++){
			mid2topic1.putIfAbsent(TopicName.get(TOPIC1).getPartition(i).toString(), sendTypedMessages(TOPIC1, SchemaType.INT32, SchemaData.INTEGER_LIST, Optional.of(i)));
			mid2topic2.putIfAbsent(TopicName.get(TOPIC2).getPartition(i).toString(), sendTypedMessages(TOPIC2, SchemaType.INT32, SchemaData.INTEGER_LIST, Optional.of(i)));
		}
		splitsByOwners = getSplitsByOwners(Arrays.asList(TOPIC1, TOPIC2), NUM_SUBTASKS);
	}

	@Test
	public void tesets() {
		System.out.println(TopicName.get(TOPIC1).getPartition(0).toString());
	}
	@Test
	public void testHandleSplitChangesAndFetch() throws IOException {
		PulsarPartitionSplitReader<Integer> reader = createReader();
		assignSplitsAndFetchUntilFinish(reader, 0);
		assignSplitsAndFetchUntilFinish(reader, 1);
	}

	@Test
	public void testWakeUp() throws InterruptedException {
		PulsarPartitionSplitReader<Integer> reader = createReader();
		AbstractPartition nonExistingTopicPartition = new BrokerPartition(new TopicRange("NotExist"));
		PulsarPartitionSplit split =
				new PulsarPartitionSplit(nonExistingTopicPartition, StartOffsetInitializer.earliest(), StopCondition.stopAfterLast());

		assignSplits(
				reader,
				Collections.singletonMap(
						split.splitId(),
						split));
		AtomicReference<Throwable> error = new AtomicReference<>();
		Thread t = new Thread(() -> {
			try {
				reader.fetch();
			} catch (Throwable e) {
				error.set(e);
			}
		}, "testWakeUp-thread");
		t.start();
		long deadline = System.currentTimeMillis() + 5000L;
		while (t.isAlive() && System.currentTimeMillis() < deadline) {
			reader.wakeUp();
			Thread.sleep(10);
		}
		assertNull(error.get());
	}

	// ------------------

	private void assignSplitsAndFetchUntilFinish(PulsarPartitionSplitReader<Integer> reader, int readerId)
			throws IOException {
		Map<String, PulsarPartitionSplit> splits = assignSplits(reader, splitsByOwners.get(readerId));

		Map<String, Integer> numConsumedRecords = new HashMap<>();
		Set<String> finishedSplits = new HashSet<>();
		while (finishedSplits.size() < splits.size()) {
			RecordsWithSplitIds<ParsedMessage<Integer>> recordsBySplitIds = reader.fetch();
			String splitId = recordsBySplitIds.nextSplit();
			while (splitId != null) {
				// Collect the records in this split.
				List<ParsedMessage<Integer>> splitFetch = new ArrayList<>();
				ParsedMessage<Integer> record;
				while ((record = recordsBySplitIds.nextRecordFromSplit()) != null) {
					splitFetch.add(record);
				}

				// Compute the expected next offset for the split.
				AbstractPartition tp = splits.get(splitId).getPartition();

				// verify the consumed records.
				if(tp.getTopic().startsWith(TopicName.get(TOPIC1).toString())){
					if (verifyConsumed(splits.get(splitId), mid2topic1, splitFetch)) {
						finishedSplits.add(splitId);
					}
				}else{
					if (verifyConsumed(splits.get(splitId), mid2topic2, splitFetch)) {
						finishedSplits.add(splitId);
					}
				}
				numConsumedRecords.compute(splitId, (ignored, recordCount) ->
														recordCount == null ? splitFetch.size() : recordCount + splitFetch.size());
				splitId = recordsBySplitIds.nextSplit();
			}
		}

		// Verify the number of records consumed from each split.
		numConsumedRecords.forEach((splitId, recordCount) -> {
			AbstractPartition tp = splits.get(splitId).getPartition();
			int expectedSize = SchemaData.INT_64_LIST.size();
			assertEquals(String.format("%s should have %d records.", splits.get(splitId), expectedSize),
					expectedSize, (int) recordCount);
		});
	}

	// ------------------

	private PulsarPartitionSplitReader<Integer> createReader() {
		ConsumerConfigurationData<byte[]> consumerConf = new ConsumerConfigurationData<>();
		consumerConf.setSubscriptionName("test-" + UUID.randomUUID());
		consumerConf.setSubscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
		consumerConf.setTopicNames(Arrays.asList(TOPIC1, TOPIC2).stream().collect(Collectors.toSet()));
		ScheduledExecutorService listenerExecutor = Executors.newScheduledThreadPool(
				1,
				r -> new Thread(r, "Pulsar listener executor"));
		return new PulsarPartitionSplitReader<>(
				new Configuration(),
				consumerConf,
				pulsarClient,
				pulsarAdmin,
				MessageDeserializer.valueOnly(new IntegerDeserializer()),
				listenerExecutor);
	}

	private Map<String, PulsarPartitionSplit> assignSplits(
			PulsarPartitionSplitReader<Integer> reader,
			Map<String, PulsarPartitionSplit> splits) {
		SplitsChange<PulsarPartitionSplit> splitsChange = new SplitsAddition<>(new ArrayList<>(splits.values()));
		reader.handleSplitsChanges(splitsChange);
		return splits;
	}

	private boolean verifyConsumed(
			final PulsarPartitionSplit split,
			Map<String, List<MessageId>> expectedMessageIdsForPartitions,
			final Collection<ParsedMessage<Integer>> consumed) {
		String topic = split.getTopic();
		List<MessageId> expectedMessageIds = expectedMessageIdsForPartitions.get(topic);
		assertEquals(expectedMessageIds.size(), consumed.size());
		int expectedOffsetIdx = 0;
		for (ParsedMessage<Integer> record : consumed) {
			assertEquals(expectedMessageIds.get(expectedOffsetIdx), record.getMessageId());
			Integer expectedValue = SchemaData.INTEGER_LIST.get(expectedOffsetIdx);
			assertEquals(expectedValue, record.getPayload());
			expectedOffsetIdx++;
		}
		return true;
	}
}
