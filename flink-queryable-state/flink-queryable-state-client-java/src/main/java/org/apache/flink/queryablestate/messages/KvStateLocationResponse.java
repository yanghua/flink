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

import org.apache.flink.annotation.Internal;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * The response containing the (serialized) kv state location sent by the {@code location service}
 * to the {@code queryable state client}.
 */
@Internal
public class KvStateLocationResponse extends MessageBody {

	private static final Logger LOG = LoggerFactory.getLogger(KvStateLocationResponse.class);

	@Nullable
	private final InetSocketAddress kvStateLocation;

	@Nullable
	private final KvStateID kvStateId;

	public KvStateLocationResponse(InetSocketAddress kvStateLocation, KvStateID kvStateId) {
		this.kvStateLocation = kvStateLocation;
		this.kvStateId = kvStateId;
	}

	@Override
	public byte[] serialize() {
		try {
			byte[] locationBytes = InstantiationUtil.serializeObject(kvStateLocation);
			byte[] kvStateIdBytes = InstantiationUtil.serializeObject(kvStateId);

			final int size = 2 * Integer.BYTES + locationBytes.length + kvStateIdBytes.length;
			return ByteBuffer.allocate(size)
				.putInt(locationBytes.length)
				.put(locationBytes)
				.putInt(kvStateIdBytes.length)
				.put(kvStateIdBytes)
				.array();
		} catch (IOException e) {
			LOG.error("Serialize KvStateLocation failed, ", e);
			return new byte[0];
		}
	}

	public InetSocketAddress getKvStateLocation() {
		return kvStateLocation;
	}

	public KvStateID getKvStateId() {
		return kvStateId;
	}

	/**
	 * A {@link MessageDeserializer deserializer} for {@link KvStateLocationResponseDeserializer}.
	 */
	public static class KvStateLocationResponseDeserializer implements MessageDeserializer<KvStateLocationResponse> {

		@Override
		public KvStateLocationResponse deserializeMessage(ByteBuf buf) {
			try {
				int locationByteLength = buf.readInt();
				Preconditions.checkArgument(locationByteLength >= 0,
					"Negative length for kv state location. " +
						"This indicates a serialization error.");
				byte[] locationBytes = new byte[locationByteLength];
				buf.readBytes(locationBytes);

				int stateIdByteLength = buf.readInt();
				Preconditions.checkArgument(stateIdByteLength >= 0,
					"Negative length for kv state id. " +
						"This indicates a serialization error.");
				byte[] stateIdBytes = new byte[stateIdByteLength];
				buf.readBytes(stateIdBytes);

				InetSocketAddress address = InstantiationUtil.deserializeObject(locationBytes, getClass().getClassLoader());
				KvStateID stateID = InstantiationUtil.deserializeObject(stateIdBytes, getClass().getClassLoader());
				return new KvStateLocationResponse(address, stateID);
			} catch (Exception e) {
				LOG.error("Deserialize KvStateLocation failed, ", e);
				return null;
			}
		}
	}

}
