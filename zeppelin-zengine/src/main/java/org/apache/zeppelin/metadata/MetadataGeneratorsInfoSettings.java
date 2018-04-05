package org.apache.zeppelin.metadata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.zeppelin.common.JsonSerializable;

import java.util.ArrayList;
import java.util.List;

public class MetadataGeneratorsInfoSettings implements JsonSerializable {
	private static final Gson gson =  new GsonBuilder().setPrettyPrinting().create();

	public List<MetadataGeneratorSetting> metadataGeneratorsSettings = new ArrayList<>();

	public String toJson() {
		return gson.toJson(this);
	}

	public static MetadataGeneratorsInfoSettings fromJson(String json) {
		return gson.fromJson(json, MetadataGeneratorsInfoSettings.class);
	}

	public List<MetadataGeneratorSetting> getMetadataGeneratorsSettings() {
		return metadataGeneratorsSettings;
	}

	public void setMetadataGeneratorSettings(
			List<MetadataGeneratorSetting> metadataGeneratorsSettings) {
		this.metadataGeneratorsSettings = metadataGeneratorsSettings;
	}
}
