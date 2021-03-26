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

package org.apache.flink.connector.pulsar.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.pulsar.sink.PulsarSinkCommittable;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.connectors.pulsar.internal.CachedPulsarClient;

import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.transaction.TransactionCoordinatorClientImpl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Committer implementation for {@link org.apache.flink.connector.pulsar.sink.PulsarSink}.
 *
 * <p>This committer is responsible for taking staged part-files, i.e. part-files in "pending" state,
 * created by the {@link org.apache.flink.connector.file.sink.writer.FileWriter FileWriter} and commit
 * them, or put them in "finished" state and ready to be consumed by downstream applications or systems.
 */
@Internal
public class PulsarCommitter implements Committer<PulsarSinkCommittable> {
	private final ClientConfigurationData clientConfigurationData;
	private final TransactionCoordinatorClientImpl tcClient;

	public PulsarCommitter(ClientConfigurationData clientConfigurationData) {
		this.clientConfigurationData = checkNotNull(clientConfigurationData);
		try {
			this.tcClient = CachedPulsarClient.getOrCreate(clientConfigurationData).getTcClient();
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<PulsarSinkCommittable> commit(List<PulsarSinkCommittable> committables) throws IOException  {

		for (PulsarSinkCommittable committable : committables) {
			tcClient.commit(committable.getTxnID(), committable.getPendingMessageIds());
		}

		return Collections.emptyList();
	}

	@Override
	public void close() throws Exception {
		tcClient.close();
	}
}
