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

package org.apache.flink.queryablestate.messages;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * The request to be sent by the {@link org.apache.flink.queryablestate.client.QueryableStateClient
 * Queryable State Client} to the dispatcher to request the location of the state.
 */
public class KvStateLocationRequest extends MessageBody {

	private final JobID jobId;
	private final String queryName;
	private final int keyHashCode;

	public KvStateLocationRequest(JobID jobId, String queryName, int keyHashCode) {
		this.jobId = Preconditions.checkNotNull(jobId);
		this.queryName = Preconditions.checkNotNull(queryName);
		this.keyHashCode = keyHashCode;
	}

	public JobID getJobId() {
		return jobId;
	}

	public String getQueryName() {
		return queryName;
	}

	public int getKeyHashCode() {
		return keyHashCode;
	}

	@Override
	public byte[] serialize() {

		byte[] serializedQueryName = queryName.getBytes(ConfigConstants.DEFAULT_CHARSET);

		// JobID + queryName + sizeOf(queryName) + hashCode
		final int size =
			JobID.SIZE +
			serializedQueryName.length + Integer.BYTES +
			Integer.BYTES;

		return ByteBuffer.allocate(size)
			.putLong(jobId.getLowerPart())
			.putLong(jobId.getUpperPart())
			.putInt(serializedQueryName.length)
			.put(serializedQueryName)
			.putInt(keyHashCode)
			.array();
	}

	/**
	 * A {@link MessageDeserializer deserializer} for {@link KvStateLocationRequest}.
	 */
	public static class KvStateLocationRequestDeserializer implements MessageDeserializer<KvStateLocationRequest> {

		@Override
		public KvStateLocationRequest deserializeMessage(ByteBuf buf) {
			JobID jobId = new JobID(buf.readLong(), buf.readLong());

			int queryNameLength = buf.readInt();
			Preconditions.checkArgument(queryNameLength >= 0,
				"Negative length for query name. " +
				"This indicates a serialization error.");

			String queryName = "";
			byte[] bytes = new byte[queryNameLength];
			buf.readBytes(bytes);
			queryName = new String(bytes, ConfigConstants.DEFAULT_CHARSET);

			int keyHashCode = buf.readInt();

			return new KvStateLocationRequest(jobId, queryName, keyHashCode);
		}

	}

}
