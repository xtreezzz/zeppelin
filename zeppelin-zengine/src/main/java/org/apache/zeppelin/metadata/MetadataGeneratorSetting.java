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
package org.apache.zeppelin.metadata;

import org.apache.zeppelin.dep.Dependency;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MetadataGeneratorSetting {
	private String id;
	private String className;
	private Integer refreshPeriodInMinutes;
	private Properties properties;
	private List<Dependency> dependencies = new ArrayList<>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public List<Dependency> getDependencies() {
		return dependencies;
	}

	public void setDependencies(List<Dependency> dependencies) {
		this.dependencies = dependencies;
	}

	public Integer getRefreshPeriodInMinutes() {
		return refreshPeriodInMinutes;
	}

	public void setRefreshPeriodInMinutes(Integer refreshPeriodInMinutes) {
		this.refreshPeriodInMinutes = refreshPeriodInMinutes;
	}
}
