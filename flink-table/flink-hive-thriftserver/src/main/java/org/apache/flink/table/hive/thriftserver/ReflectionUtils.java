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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Utils for Reflection.
 */
public class ReflectionUtils {

	public static void setSuperField(
			Object obj,
			String fieldName,
			Object fieldValue) throws RuntimeException {
		setAncestorField(obj, 1, fieldName, fieldValue);
	}

	public static void setAncestorField(
			Object obj,
			int level,
			String fieldName,
			Object fieldValue) throws RuntimeException {
		try {
			Class<?> ancestor = findNthAncestor(obj.getClass(), level);
			Field field = ancestor.getDeclaredField(fieldName);
			field.setAccessible(true);
			field.set(obj, fieldValue);
		} catch (NoSuchFieldException | IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
	}

	public static <T> T getSupperField(Object obj, String fieldName) throws RuntimeException {
		return getAncestorField(obj, 1, fieldName);
	}

	public static <T> T getAncestorField(
			Object obj,
			int level,
			String fieldName) throws RuntimeException {
		try {
			Class<?> ancestor = findNthAncestor(obj.getClass(), level);
			Field field = ancestor.getDeclaredField(fieldName);
			field.setAccessible(true);
			return (T) field.get(obj);
		} catch (NoSuchFieldException | IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
	}

	public static void invoke(
			Class<?> clazz,
			Object obj,
			String methodName,
			Class<?>[] types,
			Object[] values) throws RuntimeException {
		try {
			Method method = clazz.getDeclaredMethod(methodName, types);
			method.setAccessible(true);
			method.invoke(obj, values);
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Find nth ancestor of a clazz.
	 */
	private static Class<?> findNthAncestor(Class<?> clazz, int n) {
		Class<?> currentClz = clazz;

		while (currentClz != null) {
			Class<?> possibleClz = currentClz.getSuperclass();

			if (possibleClz != null && --n > 0) {
				currentClz = possibleClz;
				continue;
			}
			return possibleClz;
		}
		return null;
	}
}
