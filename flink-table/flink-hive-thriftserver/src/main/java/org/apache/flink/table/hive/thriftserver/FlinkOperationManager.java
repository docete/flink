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

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.session.HiveSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FlinkOperationManager extends OperationManager {

	private static final Logger LOG = LoggerFactory.getLogger(HiveThrfitServer2.class);

	public FlinkOperationManager() {
		LOG.info("Initializing FlinkOperationManager");
	}

	@Override
	public ExecuteStatementOperation newExecuteStatementOperation(HiveSession parentSession, String statement, Map<String, String> confOverlay, boolean runAsync) throws HiveSQLException {
		LOG.info("Receive new execution statement: " + statement);
		return super.newExecuteStatementOperation(parentSession, statement, confOverlay, runAsync);
	}
}
