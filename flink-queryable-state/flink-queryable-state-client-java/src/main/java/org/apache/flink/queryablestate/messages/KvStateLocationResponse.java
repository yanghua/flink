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
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * The response containing the (serialized) kv state location sent by the {@code queryable state proxy service}
 * to the {@code queryable state client}.
 */
@Internal
public class KvStateLocationResponse extends MessageBody {

	private static final Logger LOG = LoggerFactory.getLogger(KvStateLocationResponse.class);

	@Nullable
	private final InetSocketAddress kvStateLocation;

	public KvStateLocationResponse(InetSocketAddress kvStateLocation) {
		this.kvStateLocation = kvStateLocation;
	}

	@Override
	public byte[] serialize() {
		try {
			return new SerializedValue<>(kvStateLocation).getByteArray();
		} catch (IOException e) {
			LOG.error("Serialize KvStateLocation failed, ", e);
			return new byte[0];
		}
	}

	public InetSocketAddress getKvStateLocation() {
		return kvStateLocation;
	}

	public static class KvStateLocationResponseDeserializer implements MessageDeserializer<KvStateLocationResponse> {

		@Override
		public KvStateLocationResponse deserializeMessage(ByteBuf buf) {
			try {
				SerializedValue<InetSocketAddress> serializedValue = SerializedValue.fromBytes(buf.array());
				return new KvStateLocationResponse(serializedValue.deserializeValue(getClass().getClassLoader()));
			} catch (Exception e) {
				LOG.error("Deserialize KvStateLocation failed, ", e);
				return null;
			}
		}
	}

}
