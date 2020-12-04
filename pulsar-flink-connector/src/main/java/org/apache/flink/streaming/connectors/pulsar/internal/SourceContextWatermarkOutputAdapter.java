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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

/**
 * A {@link WatermarkOutput} that forwards calls to a {@link
 * SourceContext}.
 */
public class SourceContextWatermarkOutputAdapter<T> implements WatermarkOutput {
	private final SourceContext<T> sourceContext;

	public SourceContextWatermarkOutputAdapter(SourceContext<T> sourceContext) {
		this.sourceContext = sourceContext;
	}

	@Override
	public void emitWatermark(Watermark watermark) {
		sourceContext.emitWatermark(
				new org.apache.flink.streaming.api.watermark.Watermark(watermark.getTimestamp()));
	}

	@Override
	public void markIdle() {
		sourceContext.markAsTemporarilyIdle();
	}
}
