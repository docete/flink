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
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class HiveThrfitServer2 extends HiveServer2 implements ReflectedCompositeService {

	private static final Logger LOG = LoggerFactory.getLogger(HiveThrfitServer2.class);

	public HiveThrfitServer2() {
		super();
	}

	@Override
	public synchronized void init(HiveConf hiveConf) {
		FlinkCLIService cliService = new FlinkCLIService(this);
		ReflectionUtils.setSuperField(this, "cliService", cliService);
		addService(cliService);
		ThriftCLIService thriftCLIService = null;
		if (isHTTPTransportMode(hiveConf)) {
			thriftCLIService = new ThriftHttpCLIService(cliService);
		} else {
			thriftCLIService = new ThriftBinaryCLIService(cliService);
		}
		ReflectionUtils.setSuperField(this, "thriftCLIService", thriftCLIService);
		addService(thriftCLIService);

		initCompositeService(hiveConf);

	}

	public static void main(String[] args) {

		LOG.info("Start Flink ThriftServer!");

		if (Arrays.stream(args).anyMatch(s -> ("-h".equals(s) || "--help".equals(s)))) {
			HiveServer2.main(args);
			return;
		}

		// TODO: parse args and do something
		HiveConf hiveConf = new HiveConf();
		HiveThrfitServer2 server = new HiveThrfitServer2();
		server.init(hiveConf);
		server.start();
	}
}
