package org.apache.zeppelin.metadata;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.dep.DependencyResolver;
import org.apache.zeppelin.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * General class to work with meta
 */
public class MetadataServer {

	private static Logger LOGGER = LoggerFactory.getLogger(MetadataServer.class);

	private final Map<String, MetadataNode> metadataStore = new HashMap<>();
	private DependencyResolver dependencyResolver;
	private ScheduledExecutorService scheduler;

	public MetadataServer(ZeppelinConfiguration conf,
												List<MetadataGeneratorSetting> metadataGeneratorsSettingsList) {
		this.dependencyResolver = new DependencyResolver(conf.getMetadataLocalRepoDir());
		this.scheduler = Executors.newScheduledThreadPool(conf.getZeppelinMetadataSchedulerThreads());
		init(metadataGeneratorsSettingsList);
	}

	public void init(List<MetadataGeneratorSetting> metadataGeneratorsSettingsList) {
		try {
			Map<String, MetadataGenerator> metadataGenerator = PluginManager.get()
					.loadMetadataGenerators(metadataGeneratorsSettingsList, dependencyResolver);
			for (final Map.Entry<String, MetadataGenerator> metaGeneratorEntry :
					metadataGenerator.entrySet()) {
				MetadataGeneratorSetting generatorSetting = null;
				for (MetadataGeneratorSetting metadataGeneratorSetting : metadataGeneratorsSettingsList) {
					if (metadataGeneratorSetting.getId().equalsIgnoreCase(metaGeneratorEntry.getKey())) {
						generatorSetting = metadataGeneratorSetting;
						break;
					}
				}

				if (generatorSetting != null) {
					scheduler.scheduleAtFixedRate(
							new Runnable() {
									public void run() {
										MetadataNode metadataNode = metaGeneratorEntry.getValue().generate();
										metadataStore.put(metaGeneratorEntry.getKey(), metadataNode);
									}
					}, 0, generatorSetting.getRefreshPeriodInMinutes(), TimeUnit.MINUTES);
				}
			}
		} catch (IOException e) {
			LOGGER.error("Error load metadata config", e);
		}
	}

	public MetadataNode getTree(String id) {
		return metadataStore.get(id);
	}

}