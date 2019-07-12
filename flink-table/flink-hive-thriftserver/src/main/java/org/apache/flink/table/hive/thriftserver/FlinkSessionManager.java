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

package org.apache.flink.table.hive.thriftserver;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.server.HiveServer2;

import java.util.Map;

public class FlinkSessionManager extends SessionManager {

	private FlinkOperationManager operationManager = new FlinkOperationManager();

	public FlinkSessionManager(HiveServer2 hs2) {
		super(hs2);
	}

	@Override
	public synchronized void init(HiveConf hiveConf) {
		ReflectionUtils.setSuperField(this, "operationManager", operationManager);
		super.init(hiveConf);
	}

	@Override
	public SessionHandle openSession(
	  TProtocolVersion protocol,
	  String username,
	  String password,
	  String ipAddress,
	  Map<String, String> sessionConf,
	  boolean withImpersonation,
	  String delegationToken) throws HiveSQLException {
		final SessionHandle sessionHandle =
			super.openSession(protocol, username, password, ipAddress, sessionConf, withImpersonation, delegationToken);
		final HiveSession session = super.getSession(sessionHandle);

		return sessionHandle;
	}
}
