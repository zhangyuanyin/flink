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

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import akka.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.metrics.util.SystemResourcesMetricsInitializer.instantiateSystemMetrics;

/**
 * Utility class to register pre-defined metric sets.
 */
public class MetricUtils {
	private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);
	private static final String METRIC_GROUP_STATUS_NAME = "Status";
	private static final String METRICS_ACTOR_SYSTEM_NAME = "flink-metrics";

	private static final String METRIC_GROUP_HEAP_NAME = "Heap";
	private static final String METRIC_GROUP_NONHEAP_NAME = "NonHeap";

	private static final String MEMORY_USED = "Used";
	private static final String MEMORY_COMMITTED = "Committed";
	private static final String MEMORY_MAX = "Max";

	private static final String MEMORY_TYPE_EDEN = "Eden";
	private static final String MEMORY_TYPE_SURVIVOR = "Survivor";
	private static final String MEMORY_TYPE_OLD = "Old";

	private static final String MEMORY_REGEX_YOUNG = "Eden Space|PS Eden Space|Par Eden Space|G1 Eden Space";
	private static final String MEMORY_REGEX_SURVIVOR = "Survivor Space|PS Survivor Space|Par Survivor Space|G1 Survivor Space";
	private static final String MEMORY_REGEX_OLD = "Tenured Gen|PS Old Gen|CMS Old Gen|G1 Old Gen";

	private static final String STRING_FORMAT_PATTERN = "%s%s%s";
	private static final String STRING_SEPARATOR_DOT = ".";

	private MetricUtils() {
	}

	public static JobManagerMetricGroup instantiateJobManagerMetricGroup(
			final MetricRegistry metricRegistry,
			final String hostname,
			final Optional<Time> systemResourceProbeInterval) {
		final JobManagerMetricGroup jobManagerMetricGroup = new JobManagerMetricGroup(
			metricRegistry,
			hostname);

		MetricGroup statusGroup = jobManagerMetricGroup.addGroup(METRIC_GROUP_STATUS_NAME);

		// initialize the JM metrics
		instantiateStatusMetrics(statusGroup);

		if (systemResourceProbeInterval.isPresent()) {
			instantiateSystemMetrics(jobManagerMetricGroup, systemResourceProbeInterval.get());
		}
		return jobManagerMetricGroup;
	}

	public static TaskManagerMetricGroup instantiateTaskManagerMetricGroup(
			MetricRegistry metricRegistry,
			TaskManagerLocation taskManagerLocation,
			NetworkEnvironment network,
			Optional<Time> systemResourceProbeInterval) {
		final TaskManagerMetricGroup taskManagerMetricGroup = new TaskManagerMetricGroup(
			metricRegistry,
			taskManagerLocation.getHostname(),
			taskManagerLocation.getResourceID().toString());

		MetricGroup statusGroup = taskManagerMetricGroup.addGroup(METRIC_GROUP_STATUS_NAME);

		// Initialize the TM metrics
		instantiateStatusMetrics(statusGroup);

		MetricGroup networkGroup = statusGroup
			.addGroup("Network");
		instantiateNetworkMetrics(networkGroup, network);

		if (systemResourceProbeInterval.isPresent()) {
			instantiateSystemMetrics(taskManagerMetricGroup, systemResourceProbeInterval.get());
		}
		return taskManagerMetricGroup;
	}

	public static void instantiateStatusMetrics(
			MetricGroup metricGroup) {
		MetricGroup jvm = metricGroup.addGroup("JVM");

		instantiateClassLoaderMetrics(jvm.addGroup("ClassLoader"));
		instantiateGarbageCollectorMetrics(jvm.addGroup("GarbageCollector"));
		instantiateMemoryMetrics(jvm.addGroup("Memory"));
		instantiateThreadMetrics(jvm.addGroup("Threads"));
		instantiateCPUMetrics(jvm.addGroup("CPU"));
	}

	public static ActorSystem startMetricsActorSystem(Configuration configuration, String hostname, Logger logger) throws Exception {
		final String portRange = configuration.getString(MetricOptions.QUERY_SERVICE_PORT);
		final int threadPriority = configuration.getInteger(MetricOptions.QUERY_SERVICE_THREAD_PRIORITY);
		return BootstrapTools.startActorSystem(
			configuration,
			METRICS_ACTOR_SYSTEM_NAME,
			hostname,
			portRange,
			logger,
			new BootstrapTools.FixedThreadPoolExecutorConfiguration(1, 1, threadPriority));
	}

	private static void instantiateNetworkMetrics(
		MetricGroup metrics,
		final NetworkEnvironment network) {

		final NetworkBufferPool networkBufferPool = network.getNetworkBufferPool();
		metrics.<Integer, Gauge<Integer>>gauge("TotalMemorySegments", networkBufferPool::getTotalNumberOfMemorySegments);
		metrics.<Integer, Gauge<Integer>>gauge("AvailableMemorySegments", networkBufferPool::getNumberOfAvailableMemorySegments);
	}

	private static void instantiateClassLoaderMetrics(MetricGroup metrics) {
		final ClassLoadingMXBean mxBean = ManagementFactory.getClassLoadingMXBean();
		metrics.<Long, Gauge<Long>>gauge("ClassesLoaded", mxBean::getTotalLoadedClassCount);
		metrics.<Long, Gauge<Long>>gauge("ClassesUnloaded", mxBean::getUnloadedClassCount);
	}

	private static void instantiateGarbageCollectorMetrics(MetricGroup metrics) {
		List<GarbageCollectorMXBean> garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();

		for (final GarbageCollectorMXBean garbageCollector: garbageCollectors) {
			MetricGroup gcGroup = metrics.addGroup(garbageCollector.getName());

			gcGroup.<Long, Gauge<Long>>gauge("Count", garbageCollector::getCollectionCount);
			gcGroup.<Long, Gauge<Long>>gauge("Time", garbageCollector::getCollectionTime);
		}
	}

	/**
	 * Cause the jvm heap/non-heap memory be reported only once, change the way of report to {@link ManagementFactory#getMemoryPoolMXBeans}.
	 */
	private static void instantiateMemoryMetrics(MetricGroup metrics) {
		// Rebuild this part for reporting needed heap information (Attention: maybe it's a bug code at front logic).
		List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
		instantiateHeapMemoryMetrics(metrics.addGroup(METRIC_GROUP_HEAP_NAME), memoryPoolMXBeans);
		instantiateNonHeapMemoryMetrics(metrics.addGroup(METRIC_GROUP_NONHEAP_NAME), memoryPoolMXBeans);

		final MBeanServer con = ManagementFactory.getPlatformMBeanServer();

		final String directBufferPoolName = "java.nio:type=BufferPool,name=direct";

		try {
			final ObjectName directObjectName = new ObjectName(directBufferPoolName);

			MetricGroup direct = metrics.addGroup("Direct");

			direct.<Long, Gauge<Long>>gauge("Count", new AttributeGauge<>(con, directObjectName, "Count", -1L));
			direct.<Long, Gauge<Long>>gauge("MemoryUsed", new AttributeGauge<>(con, directObjectName, "MemoryUsed", -1L));
			direct.<Long, Gauge<Long>>gauge("TotalCapacity", new AttributeGauge<>(con, directObjectName, "TotalCapacity", -1L));
		} catch (MalformedObjectNameException e) {
			LOG.warn("Could not create object name {}.", directBufferPoolName, e);
		}

		final String mappedBufferPoolName = "java.nio:type=BufferPool,name=mapped";

		try {
			final ObjectName mappedObjectName = new ObjectName(mappedBufferPoolName);

			MetricGroup mapped = metrics.addGroup("Mapped");

			mapped.<Long, Gauge<Long>>gauge("Count", new AttributeGauge<>(con, mappedObjectName, "Count", -1L));
			mapped.<Long, Gauge<Long>>gauge("MemoryUsed", new AttributeGauge<>(con, mappedObjectName, "MemoryUsed", -1L));
			mapped.<Long, Gauge<Long>>gauge("TotalCapacity", new AttributeGauge<>(con, mappedObjectName, "TotalCapacity", -1L));
		} catch (MalformedObjectNameException e) {
			LOG.warn("Could not create object name {}.", mappedBufferPoolName, e);
		}
	}

	/**
	 * deal with heap memory metrics relatives.
	 *
	 * @param metrics
	 * @param memoryPoolMXBeans
	 */
	@VisibleForTesting
	private static void instantiateHeapMemoryMetrics(MetricGroup metrics, List<MemoryPoolMXBean> memoryPoolMXBeans) {
		List<MemoryPoolMXBean> heapMemoryPoolMXBeans;
		doInstantiateMemoryMetrics(metrics, heapMemoryPoolMXBeans = memoryPoolMXBeans.stream().filter(pool -> pool.getType().equals(MemoryType.HEAP)).collect(Collectors.toList()));
		doInstantiateMemoryMetrics(metrics, heapMemoryPoolMXBeans, MemoryType.HEAP);
	}

	/**
	 * deal with non-heap memory metrics relatives.
	 *
	 * @param metrics
	 * @param memoryPoolMXBeans
	 */
	@VisibleForTesting
	private static void instantiateNonHeapMemoryMetrics(MetricGroup metrics, List<MemoryPoolMXBean> memoryPoolMXBeans) {
		doInstantiateMemoryMetrics(metrics, memoryPoolMXBeans.stream().filter(pool -> pool.getType().equals(MemoryType.NON_HEAP)).collect(Collectors.toList()));
	}

	/**
	 * perform to report jvm memory metric.
	 *
	 * @param metrics
	 * @param memoryPoolMxBeans
	 */
	private static void doInstantiateMemoryMetrics(MetricGroup metrics, List<MemoryPoolMXBean> memoryPoolMxBeans) {
		metrics.<Long, Gauge<Long>>gauge(MEMORY_USED, new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans.stream().map(memory -> memory.getUsage().getUsed()).reduce((sum, memory) -> sum + memory).get();
			}
		});
		metrics.<Long, Gauge<Long>>gauge(MEMORY_COMMITTED, new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans.stream().map(memory -> memory.getUsage().getCommitted()).reduce((sum, memory) -> sum + memory).get();
			}
		});
		metrics.<Long, Gauge<Long>>gauge(MEMORY_MAX, new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans.stream().map(memory -> memory.getUsage().getMax()).reduce((sum, memory) -> sum + memory).get();
			}
		});
	}

	/**
	 * perform to add eden, survivor, old memory space report only.
	 *
	 * @param metrics
	 * @param memoryPoolMxBeans
	 */
	private static void doInstantiateMemoryMetrics(MetricGroup metrics, List<MemoryPoolMXBean> memoryPoolMxBeans, Enum memoryType) {
		/**
		 * 1. report eden memory relatives.
		 */
		metrics.<Long, Gauge<Long>>gauge(String.format(STRING_FORMAT_PATTERN, MEMORY_TYPE_EDEN, STRING_SEPARATOR_DOT, MEMORY_USED), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans
					.stream()
					.filter(memory -> memory.getName().matches(MEMORY_REGEX_YOUNG))
					.map(memory -> memory.getUsage().getUsed()).reduce((sum, memory) -> sum + memory)
					.get();
			}
		});

		metrics.<Long, Gauge<Long>>gauge(String.format(STRING_FORMAT_PATTERN, MEMORY_TYPE_EDEN, STRING_SEPARATOR_DOT, MEMORY_COMMITTED), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans
					.stream()
					.filter(memory -> memory.getName().matches(MEMORY_REGEX_YOUNG))
					.map(memory -> memory.getUsage().getCommitted()).reduce((sum, memory) -> sum + memory)
					.get();
			}
		});

		metrics.<Long, Gauge<Long>>gauge(String.format(STRING_FORMAT_PATTERN, MEMORY_TYPE_EDEN, STRING_SEPARATOR_DOT, MEMORY_MAX), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans
					.stream()
					.filter(memory -> memory.getName().matches(MEMORY_REGEX_YOUNG))
					.map(memory -> memory.getUsage().getMax()).reduce((sum, memory) -> sum + memory)
					.get();
			}
		});

		/**
		 * 2. report survivor memory relatives.
		 */
		metrics.<Long, Gauge<Long>>gauge(String.format(STRING_FORMAT_PATTERN, MEMORY_TYPE_SURVIVOR, STRING_SEPARATOR_DOT, MEMORY_USED), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans
					.stream()
					.filter(memory -> memory.getName().matches(MEMORY_REGEX_SURVIVOR))
					.map(memory -> memory.getUsage().getUsed()).reduce((sum, memory) -> sum + memory)
					.get();
			}
		});

		metrics.<Long, Gauge<Long>>gauge(String.format(STRING_FORMAT_PATTERN, MEMORY_TYPE_SURVIVOR, STRING_SEPARATOR_DOT, MEMORY_COMMITTED), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans
					.stream()
					.filter(memory -> memory.getName().matches(MEMORY_REGEX_SURVIVOR))
					.map(memory -> memory.getUsage().getCommitted()).reduce((sum, memory) -> sum + memory)
					.get();
			}
		});

		metrics.<Long, Gauge<Long>>gauge(String.format(STRING_FORMAT_PATTERN, MEMORY_TYPE_SURVIVOR, STRING_SEPARATOR_DOT, MEMORY_MAX), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans
					.stream()
					.filter(memory -> memory.getName().matches(MEMORY_REGEX_SURVIVOR))
					.map(memory -> memory.getUsage().getMax()).reduce((sum, memory) -> sum + memory)
					.get();
			}
		});

		/**
		 * 3. report old memory relatives.
		 */
		metrics.<Long, Gauge<Long>>gauge(String.format(STRING_FORMAT_PATTERN, MEMORY_TYPE_OLD, STRING_SEPARATOR_DOT, MEMORY_USED), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans
					.stream()
					.filter(memory -> memory.getName().matches(MEMORY_REGEX_OLD))
					.map(memory -> memory.getUsage().getUsed()).reduce((sum, memory) -> sum + memory)
					.get();
			}
		});

		metrics.<Long, Gauge<Long>>gauge(String.format(STRING_FORMAT_PATTERN, MEMORY_TYPE_OLD, STRING_SEPARATOR_DOT, MEMORY_COMMITTED), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans
					.stream()
					.filter(memory -> memory.getName().matches(MEMORY_REGEX_OLD))
					.map(memory -> memory.getUsage().getCommitted()).reduce((sum, memory) -> sum + memory)
					.get();
			}
		});

		metrics.<Long, Gauge<Long>>gauge(String.format(STRING_FORMAT_PATTERN, MEMORY_TYPE_OLD, STRING_SEPARATOR_DOT, MEMORY_MAX), new Gauge<Long>() {
			@Override
			public Long getValue() {
				return memoryPoolMxBeans
					.stream()
					.filter(memory -> memory.getName().matches(MEMORY_REGEX_OLD))
					.map(memory -> memory.getUsage().getMax()).reduce((sum, memory) -> sum + memory)
					.get();
			}
		});
	}

	private static void instantiateThreadMetrics(MetricGroup metrics) {
		final ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

		metrics.<Integer, Gauge<Integer>>gauge("Count", mxBean::getThreadCount);
	}

	private static void instantiateCPUMetrics(MetricGroup metrics) {
		try {
			final com.sun.management.OperatingSystemMXBean mxBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

			metrics.<Double, Gauge<Double>>gauge("Load", mxBean::getProcessCpuLoad);
			metrics.<Long, Gauge<Long>>gauge("Time", mxBean::getProcessCpuTime);
		} catch (Exception e) {
			LOG.warn("Cannot access com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()" +
				" - CPU load metrics will not be available.", e);
		}
	}

	private static final class AttributeGauge<T> implements Gauge<T> {
		private final MBeanServer server;
		private final ObjectName objectName;
		private final String attributeName;
		private final T errorValue;

		private AttributeGauge(MBeanServer server, ObjectName objectName, String attributeName, T errorValue) {
			this.server = Preconditions.checkNotNull(server);
			this.objectName = Preconditions.checkNotNull(objectName);
			this.attributeName = Preconditions.checkNotNull(attributeName);
			this.errorValue = errorValue;
		}

		@SuppressWarnings("unchecked")
		@Override
		public T getValue() {
			try {
				return (T) server.getAttribute(objectName, attributeName);
			} catch (MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException e) {
				LOG.warn("Could not read attribute {}.", attributeName, e);
				return errorValue;
			}
		}
	}
}
