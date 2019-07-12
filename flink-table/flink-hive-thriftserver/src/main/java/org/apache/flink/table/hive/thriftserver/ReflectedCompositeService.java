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
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.Service;

import java.util.List;

public interface ReflectedCompositeService {
	default void initCompositeService(HiveConf hiveConf) throws RuntimeException {

		final List<Service> services = ReflectionUtils.getAncestorField(this, 2, "serviceList");
		services.stream().forEach((service -> service.init(hiveConf)));

		final Class<?>[] STATES = { Service.STATE.class };
		final Object[] NOTINITED = { Service.STATE.NOTINITED };
		final Object[] INITED = { Service.STATE.INITED };

		ReflectionUtils.invoke(
			AbstractService.class,
			this,
			"ensureCurrentState",
			STATES, NOTINITED);
		ReflectionUtils.setAncestorField(this, 3, "hiveConf", hiveConf);
		ReflectionUtils.invoke(
			AbstractService.class,
			this,
			"changeState",
			STATES, INITED);
	}
}
