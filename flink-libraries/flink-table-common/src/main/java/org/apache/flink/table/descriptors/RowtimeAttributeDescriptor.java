/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.descriptors;

import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.table.sources.WatermarkStrategy;

import java.util.Objects;

/**
 * Describes a rowtime attribute of a [[TableSource]].
 */
public class RowtimeAttributeDescriptor {

	/** The name of the rowtime attribute. **/
	private String attributeName;

	/** The timestamp extractor to derive the values of the attribute. **/
	private TimestampExtractor timestampExtractor;

	/** The watermark strategy associated with the attribute. **/
	private WatermarkStrategy watermarkStrategy;

	public RowtimeAttributeDescriptor(
		String attributeName,
		TimestampExtractor timestampExtractor,
		WatermarkStrategy watermarkStrategy) {

		this.attributeName = attributeName;
		this.timestampExtractor = timestampExtractor;
		this.watermarkStrategy = watermarkStrategy;
	}

	/** Returns the name of the rowtime attribute. */
	public String getAttributeName() {
		return attributeName;
	}

	/** Returns the [[TimestampExtractor]] for the attribute. */
	public TimestampExtractor getTimestampExtractor() {
		return timestampExtractor;
	}

	/** Returns the [[WatermarkStrategy]] for the attribute. */
	public WatermarkStrategy getWatermarkStrategy() {
		return watermarkStrategy;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof RowtimeAttributeDescriptor) {
			RowtimeAttributeDescriptor that = (RowtimeAttributeDescriptor) other;
			return Objects.equals(attributeName, that.attributeName) &&
				Objects.equals(timestampExtractor, that.timestampExtractor) &&
				Objects.equals(watermarkStrategy, that.watermarkStrategy);
		}

		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(attributeName, timestampExtractor, watermarkStrategy);
	}



}
