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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.sinks.CollectRowTableSink;
import org.apache.flink.table.sinks.CollectTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.session.HiveSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FlinkExecuteStatementOperation extends ExecuteStatementOperation {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkExecuteStatementOperation.class);

	private final TableEnvironment sqlContext;

	private String statementId;

	private Table table;
	private TableSchema tableSchema;
	private List<Row> resultList;
	private Iterator<Row> iter;

	public FlinkExecuteStatementOperation(
	  HiveSession parentSession,
	  String statement,
	  Map<String, String> confOverlay,
	  Boolean runInBackground,
	  TableEnvironment sqlContext) {
		super(parentSession, statement, confOverlay, runInBackground);
		this.sqlContext = sqlContext;
	}

	@Override
	protected void runInternal() throws HiveSQLException {
		setState(OperationState.PENDING);
		setHasResultSet(true);

		if (!shouldRunAsync()) {
			execute();
		} else {
			final UserGroupInformation currentUGI = getCurrentUGI();

			// Runnable impl to call runInternal asynchronously
			// from a different thread
			Runnable backgroundOperation = new Runnable() {
				@Override
				public void run() {
					PrivilegedExceptionAction<Object> doAsAction = new PrivilegedExceptionAction<Object>() {
						@Override
						public Object run() throws HiveSQLException {
							registerCurrentOperationLog();

							try {
								execute();
							} catch (HiveSQLException e) {
								setOperationException(e);
								LOG.error("Error running SQL query: ", e);
							} finally {
								unregisterOperationLog();
							}
							return null;
						}
					};

					try {
						currentUGI.doAs(doAsAction);
					} catch (Exception e) {
						setOperationException(new HiveSQLException(e));
						LOG.error("Error running SQL query as user : " + currentUGI.getShortUserName(), e);
					}
				}
			};

			try {
				// This submit blocks if no background threads are available to run this operation
				Future<?> backgroundHandle =
					getParentSession().getSessionManager().submitBackgroundOperation(backgroundOperation);
				setBackgroundHandle(backgroundHandle);
			} catch (RejectedExecutionException rejected) {
				setState(OperationState.ERROR);
				throw new HiveSQLException("The background threadpool connot accept" +
					" new task for execution, please retry the operation", rejected);
			}
		}
	}

	@Override
	public org.apache.hive.service.cli.TableSchema getResultSetSchema() throws HiveSQLException {
		if (table == null || tableSchema == null || tableSchema.getFieldCount() == 0) {
			return new org.apache.hive.service.cli.TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")));
		} else {
			final String[] fieldNames = tableSchema.getFieldNames();
			final DataType[] fieldTypes = tableSchema.getFieldDataTypes();
			final List<FieldSchema> schema =
				IntStream
				.range(0, tableSchema.getFieldCount())
				.mapToObj((i) -> {
					final String hiveTypeString = fieldTypes[i].toString();
					return new FieldSchema(fieldNames[i], hiveTypeString, "");
				}).collect(Collectors.toList());
			return new org.apache.hive.service.cli.TableSchema(schema);
		}
	}

	@Override
	public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
		validateDefaultFetchOrientation(orientation);
		assertState(OperationState.FINISHED);
		setHasResultSet(true);

		RowSet rowSet = RowSetFactory.create(getResultSetSchema(), getProtocolVersion());

		if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
			iter = resultList.iterator();
		}

		if (!iter.hasNext()) {
			return rowSet;
		} else {
			long curRow = 0;
			while (curRow < maxRows && iter.hasNext()) {
				final Row row = iter.next();
				final List<Object> fields =
					IntStream
						.range(0, row.getArity())
						.mapToObj(i -> row.getField(i))
						.collect(Collectors.toList());
				rowSet.addRow(fields.toArray());
				curRow += 1;
			}
			return rowSet;
		}
	}

	@Override
	public void close() throws HiveSQLException {
		LOG.info("CLOSING " + statementId);
		cleanup(OperationState.CLOSED);
		cleanupOperationLog();
	}

	private void execute() throws HiveSQLException {
		statementId = UUID.randomUUID().toString();
		final String statement = getStatement();
		LOG.info("execute statement " + statement + " with " + statementId);
		setState(OperationState.RUNNING);

		try {
			// TODO: support other deploy modes
			// TODO: support streaming mode
			table = sqlContext.sqlQuery(statement);
			tableSchema = removeTimeAttributes(table.getSchema());
			final RowTypeInfo outputType = new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
			final TypeSerializer<Row> serializer = outputType.createSerializer(new ExecutionConfig());
			final CollectTableSink<Row> sink = (CollectTableSink<Row>) new CollectRowTableSink().configure(tableSchema.getFieldNames(), tableSchema.getFieldTypes());
			final String id = new AbstractID().toString();
			sink.init(serializer, id);
			sqlContext.registerTableSink(statementId, sink);
			sqlContext.insertInto(table, statementId);

			final JobExecutionResult result = sqlContext.execute(statement);
			final ArrayList<byte[]> accResult = result.getAccumulatorResult(id);
			resultList = SerializedListAccumulator.deserializeList(accResult, serializer);
			iter = resultList.iterator();
			setState(OperationState.FINISHED);
		} catch (HiveSQLException e) {
			if (getStatus().getState() == OperationState.CANCELED) {
				return;
			} else {
				setState(OperationState.ERROR);
				throw e;
			}
		} catch (Exception e) {
			LOG.error("Error executing query: ", e);
			setState(OperationState.ERROR);
			throw new HiveSQLException(e);
		}
	}

	private UserGroupInformation getCurrentUGI() throws HiveSQLException {
		try {
			return Utils.getUGI();
		} catch (Exception e) {
			throw new HiveSQLException("Unable to get current user", e);
		}
	}

	private void registerCurrentOperationLog() {
		if (isOperationLogEnabled) {
			if (operationLog == null) {
				LOG.warn("Failed to get current OperationLog object of Operation: " +
					getHandle().getHandleIdentifier());
				isOperationLogEnabled = false;
				return;
			}
			OperationLog.setCurrentOperationLog(operationLog);
		}
	}

	private void cleanup(OperationState state) throws HiveSQLException {
		setState(state);
		if (shouldRunAsync()) {
			Future<?> backgroundHandle = getBackgroundHandle();
			if (backgroundHandle != null) {
				backgroundHandle.cancel(true);
			}
		}

		// TODO: should cleanup running job
	}

	private static TableSchema removeTimeAttributes(TableSchema schema) {
		final TableSchema.Builder builder = TableSchema.builder();
		for (int i = 0; i < schema.getFieldCount(); i++) {
			final TypeInformation<?> type = schema.getFieldTypes()[i];
			final TypeInformation<?> convertedType;
			if (FlinkTypeFactory.isTimeIndicatorType(type)) {
				convertedType = Types.SQL_TIMESTAMP;
			} else {
				convertedType = type;
			}
			builder.field(schema.getFieldNames()[i], convertedType);
		}
		return builder.build();
	}
}
