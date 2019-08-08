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

package org.apache.flink.runtime.query;

import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;

/**
 * An interface for Queryable State Proxy Service running on Dispatcher in the cluster
 * or on a single Queryable State Proxy Server.
 * The proxy service is responsible for serving location metadata requests.
 */
public interface QueryableStateProxyService {

	/** Starts the server. */
	void start() throws Throwable;

	/** Shuts down the server and all related thread pools. */
	void shutdown();

	void setHighAvailabilityServices(HighAvailabilityServices haService);

	HighAvailabilityServices getHighAvailabilityServices();

	void setDispatcherGateway(DispatcherGateway dispatcherGateway);

	DispatcherGateway getDispatcherGateway();

}
