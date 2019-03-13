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

package org.apache.zeppelin.interpreterV2.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.Dependency;
import org.apache.zeppelin.DependencyResolver;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.repository.RemoteRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * InterpreterSettingManager - создать InterpreterSettingRepository который умеет загружать
 * настройки из файла\сохранять в файл\отдавать настройки по shebang
 *
 * примерный апи:
 *  >get(String shebang)
 *  >getAll()
 *  >update(IntSetting setting)
 *  >delete(String shebang)
 *  >delete(IntSetting setting)
 *  >persist(IntSetting setting)
 */
@Component
public class InterpreterSettingRepository {
  private static final Logger LOGGER = LoggerFactory.getLogger(InterpreterSettingRepository.class);
  private static final Map<String, Object> DEFAULT_EDITOR = ImmutableMap.of(
      "language", (Object) "text",
      "editOnDblClick", false);
  private static final String FILENAME = "/interpreter.json";


  private final ZeppelinConfiguration conf;
  private final DependencyResolver dependencyResolver;

  /**
   * This is used by creating and running Interpreters
   * shebang --> InterpreterSetting
   */
  private final Map<String, InterpreterSettingV2> interpreterSettings = Maps.newConcurrentMap();

  /**
   * This is only InterpreterSetting templates with default name and properties
   * group --> InterpreterSetting
   */
  private final Map<String, InterpreterSettingV2> interpreterSettingTemplates =
      Maps.newConcurrentMap();

  private final List<RemoteRepository> interpreterRepositories;
  private InterpreterOption defaultOption;
  private String defaultInterpreterGroup;

  @Autowired
  public InterpreterSettingRepository(final ZeppelinConfiguration zeppelinConfiguration) {
    this(zeppelinConfiguration, new InterpreterOption());
  }

  public InterpreterSettingRepository(final ZeppelinConfiguration conf,
      final InterpreterOption defaultOption) {
    this.conf = conf;
    this.defaultOption = defaultOption;
    LOGGER.debug("InterpreterRootPath: {}", Paths.get(conf.getInterpreterDir()));
    this.dependencyResolver =
        new DependencyResolver(conf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_LOCALREPO));
    this.interpreterRepositories = dependencyResolver.getRepos();
    this.defaultInterpreterGroup =
        conf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_DEFAULT);

    try {
      init();
    } catch (final Exception e) {
      LOGGER.error("FAIL" , e);
    }
  }

  private void init() {
    loadInterpreterSettingFromDefaultDir(true);
    loadFromFile();
    saveToFile();
  }


  //----------------------------- API -----------------------------
  public InterpreterSettingV2 get(String id) {
    return interpreterSettings.get(id);
  }

  /**
   * Get interpreter settings
   */
  public List<InterpreterSettingV2> get() {
    List<InterpreterSettingV2> orderedSettings = new ArrayList<>(interpreterSettings.values());
    Collections.sort(orderedSettings, new Comparator<InterpreterSettingV2>() {
      @Override
      public int compare(InterpreterSettingV2 o1, InterpreterSettingV2 o2) {
        if (o1.getName().equals(defaultInterpreterGroup)) {
          return -1;
        } else if (o2.getName().equals(defaultInterpreterGroup)) {
          return 1;
        } else {
          return o1.getName().compareTo(o2.getName());
        }
      }
    });
    return orderedSettings;
  }

  public InterpreterSettingV2 createNewSetting(String name, String group,
      List<Dependency> dependencies, InterpreterOption option, Map<String, InterpreterPropertyOld> p)
      throws IOException {

    if (name.contains(".")) {
      throw new IOException("'.' is invalid for InterpreterSetting name.");
    }
    // check if name is existed
    for (InterpreterSettingV2 interpreterSetting : interpreterSettings.values()) {
      if (interpreterSetting.getName().equals(name)) {
        throw new IOException("Interpreter " + name + " already existed");
      }
    }

    //List<InterpreterInfo> infos = interpreterSettingTemplates.get(group).getInterpreterInfos();
    //InterpreterSettingV2 setting = new InterpreterSettingV2(name, name, group, p, infos, dependencies, option);
    //interpreterSettings.put(setting.getId(), setting);
    saveToFile();
    return null;
  }

  public Map<String, InterpreterSettingV2> getInterpreterSettingTemplates() {
    return interpreterSettingTemplates;
  }

  //----------------------------- LOADING -----------------------------
  /**
   * 1. detect interpreter setting via interpreter-setting.json in each interpreter folder
   * 2. detect interpreter setting in interpreter.json that is saved before
   *
   * Register interpreter by the following ordering:
   *    1. Register it from path {ZEPPELIN_HOME}/interpreter/{interpreter_name}/
   *       interpreter-setting.json
   *    2. Register it from interpreter-setting.json in classpath
   *       {ZEPPELIN_HOME}/interpreter/{interpreter_name}
   *
   * @param override
   * @throws IOException
   */
  private void loadInterpreterSettingFromDefaultDir(boolean override) {
    String interpreterJson = conf.getInterpreterJson();

    if (Paths.get(conf.getInterpreterDir()).toFile().exists()) {
      try (DirectoryStream<Path> interpreterDirs = Files.newDirectoryStream(
          Paths.get(conf.getInterpreterDir()), entry ->
              entry.toFile().exists() && entry.toFile().isDirectory())) {

        for (Path interpreterDir : interpreterDirs) {
          String interpreterDirString = interpreterDir.toString();
          if (!registerInterpreterFromPath(interpreterDirString, interpreterJson, override)
              && !registerInterpreterFromResource(interpreterDirString, interpreterJson,
              override)) {
            LOGGER.warn("No interpreter-setting.json found in {}", interpreterDirString);
          }
        }
      } catch (IOException e) {
        LOGGER.error("Failed to load interpreter setting from default directory - {}",
            conf.getInterpreterDir(), e);
      }
    } else {
      LOGGER.warn("InterpreterDir {} doesn't exist", conf.getInterpreterDir());
    }
  }

  private boolean registerInterpreterFromPath(String interpreterDir, String interpreterJson,
      boolean override) {
    Path interpreterJsonPath = Paths.get(interpreterDir, interpreterJson);
    if (interpreterJsonPath.toFile().exists()) {
      LOGGER.debug("Reading interpreter-setting.json from file {}", interpreterJsonPath);
      try (FileInputStream json = new FileInputStream(interpreterJsonPath.toFile())) {
        List<BaseInterpreterConfig> baseInterpreterConfigList = getInterpreterListFromJson(json);
        registerInterpreterSetting(baseInterpreterConfigList, override);
        return true;
      } catch (IOException e) {
        LOGGER.error("Failed to register interpreter from path - {}", interpreterJsonPath, e);
      }
    }
    return false;
  }

  /**
   * Input stream should be closed in caller method!
   */
  private List<BaseInterpreterConfig> getInterpreterListFromJson(final InputStream stream) {
    Type registeredInterpreterListType = new TypeToken<List<BaseInterpreterConfig>>() {}.getType();
    return new GsonBuilder().setPrettyPrinting().create().fromJson(
        new InputStreamReader(stream), registeredInterpreterListType);
  }

  private void registerInterpreterSetting(final List<BaseInterpreterConfig> baseInterpreterConfigs,
      final boolean override) {

//    Map<String, InterpreterProperty> properties = new HashMap<>();
//    List<InterpreterInfo> interpreterInfos = new ArrayList<>();
//    InterpreterOption option = defaultOption;
//    String group = null;
//    for (BaseInterpreterConfig baseInterpreterConfig : baseInterpreterConfigs) {
////      InterpreterInfo interpreterInfo =
////          new InterpreterInfo(baseInterpreterConfig.getClassName(), baseInterpreterConfig.getName(),
////              baseInterpreterConfig.isDefaultInterpreter(), baseInterpreterConfig.getEditor());
////      group = baseInterpreterConfig.getGroup();
////      // use defaultOption if it is not specified in interpreter-setting.json
////      if (baseInterpreterConfig.getOption() != null) {
////        option = baseInterpreterConfig.getOption();
////      }
////      properties.putAll(baseInterpreterConfig.getProperties());
////      interpreterInfos.add(interpreterInfo);
//    }
//    //InterpreterSettingV2 interpreterSettingTemplate = new InterpreterSettingV2(
//    //    group, group, group, properties, interpreterInfos, new ArrayList<>(), option);
//
//    String key = interpreterSettingTemplate.getName();
//    if(override || !interpreterSettingTemplates.containsKey(key)) {
//      LOGGER.info("Register InterpreterSettingTemplate: {}", key);
//      interpreterSettingTemplates.put(key, interpreterSettingTemplate);
//    }
  }

  private boolean registerInterpreterFromResource(final String interpreterDir,
      final String interpreterJson, final boolean override) {
    try {
      URL[] urls = recursiveBuildLibList(new File(interpreterDir));
      ClassLoader tempClassLoader = new URLClassLoader(urls, null);
      URL url = tempClassLoader.getResource(interpreterJson);
      if (url == null) {
        return false;
      }
      return readInterpreterSettingJsonByUrl(url, override);
    } catch (MalformedURLException e) {
      LOGGER.error("Failed to register interpreter from resource", e);
    }
    return false;
  }

  private boolean readInterpreterSettingJsonByUrl(final URL url, final boolean override) {
    LOGGER.debug("Reading interpreter-setting.json from {} as Resource", url);
    try (InputStream stream = url.openStream()) {
      List<BaseInterpreterConfig> baseInterpreterConfigList = getInterpreterListFromJson(stream);
      registerInterpreterSetting(baseInterpreterConfigList, override);
      return true;
    } catch (IOException e) {
      LOGGER.error("Failed to read resource file", e);
      return false;
    }
  }

  private URL[] recursiveBuildLibList(final File path) throws MalformedURLException {
    URL[] urls = new URL[0];
    if (path == null || !path.exists()) {
      return urls;
    } else if (path.getName().startsWith(".")) {
      return urls;
    } else if (path.isDirectory()) {
      File[] files = path.listFiles();
      if (files != null) {
        for (File f : files) {
          urls = (URL[]) ArrayUtils.addAll(urls, recursiveBuildLibList(f));
        }
      }
      return urls;
    } else {
      return new URL[]{path.toURI().toURL()};
    }
  }

  /**
   * Load interpreter setting from interpreter.json
   */
  private void loadFromFile() {
    File file = new File(conf.getConfigFSDir() + FILENAME);
    InterpreterInfoSaving infoSaving = null;
    try (FileInputStream stream = new FileInputStream(file)) {
      String json = IOUtils.toString(stream, "UTF-8");
      LOGGER.info("Load Interpreter Setting from file: {}", file);
      infoSaving = new Gson().fromJson(json, InterpreterInfoSaving.class);
    } catch (IOException e) {
      LOGGER.warn("Interpreter Setting file {} is not existed", file);
    }

    if (infoSaving == null) {
      // it is fresh zeppelin instance if there's no interpreter.json, just create interpreter
      // setting from interpreterSettingTemplates
      for (InterpreterSettingV2 interpreterSettingTemplate : interpreterSettingTemplates.values()) {
        InterpreterSettingV2 interpreterSetting = new InterpreterSettingV2(
            interpreterSettingTemplate.getId(), interpreterSettingTemplate.getName(),
            interpreterSettingTemplate.getGroup(), interpreterSettingTemplate.getProperties(),
            interpreterSettingTemplate.getInterpreterInfos(),
            interpreterSettingTemplate.getDependencies(), interpreterSettingTemplate.getOption());
        //TODO(egorklimov): shebang <=> group?
        interpreterSettings.put(interpreterSetting.getId(), interpreterSetting);
      }
      return;
    }

    //TODO(zjffdu) still ugly (should move all to InterpreterInfoSaving)
    for (InterpreterSettingV2 savedInterpreterSetting : infoSaving.interpreterSettings.values()) {
      savedInterpreterSetting.setProperties(convertInterpreterProperties(
          savedInterpreterSetting.getProperties()
      ));

      InterpreterSettingV2 interpreterSettingTemplate =
          interpreterSettingTemplates.get(savedInterpreterSetting.getGroup());
      // InterpreterSettingTemplate is from interpreter-setting.json which represent the latest
      // InterpreterSetting, while InterpreterSetting is from interpreter.json which represent
      // the user saved interpreter setting
      if (interpreterSettingTemplate != null) {
        // merge properties from interpreter-setting.json and interpreter.json
        Map<String, InterpreterPropertyOld> mergedProperties = convertInterpreterProperties(
            interpreterSettingTemplate.getProperties());
        Map<String, InterpreterPropertyOld> savedProperties = convertInterpreterProperties(
            savedInterpreterSetting.getProperties());

        for (Map.Entry<String, InterpreterPropertyOld> entry : savedProperties.entrySet()) {
          // only merge properties whose value is not empty
          if (entry.getValue().getValue() != null && !
              StringUtils.isBlank(entry.getValue().toString())) {
            mergedProperties.put(entry.getKey(), entry.getValue());
          }
        }

        savedInterpreterSetting.setProperties(mergedProperties);
        // merge InterpreterInfo
        savedInterpreterSetting.setInterpreterInfos(
            interpreterSettingTemplate.getInterpreterInfos());
      } else {
        LOGGER.warn("No InterpreterSetting Template found for InterpreterSetting: {}, but it is "
            + "found in interpreter.json, it would be skipped.", savedInterpreterSetting.getGroup());
        continue;
      }

      // Overwrite the default InterpreterSetting we registered from InterpreterSetting Templates
      // remove it first
      for (InterpreterSettingV2 setting : interpreterSettings.values()) {
        if (setting.getName().equals(savedInterpreterSetting.getName())) {
          interpreterSettings.remove(setting.getId());
        }
      }
      LOGGER.info("Create Interpreter Setting {} from interpreter.json",
          savedInterpreterSetting.getName());
      interpreterSettings.put(savedInterpreterSetting.getId(), savedInterpreterSetting);
    }

    if (infoSaving.interpreterRepositories != null) {
      for (RemoteRepository repo : infoSaving.interpreterRepositories) {
        if (!dependencyResolver.getRepos().contains(repo)) {
          this.interpreterRepositories.add(repo);
        }
      }

      // force interpreter dependencies loading once the
      // repositories have been loaded.
      for (InterpreterSettingV2 setting : interpreterSettings.values()) {
        setting.setDependencies(setting.getDependencies());
      }
    }
  }

  //----------------------------- SAVING -----------------------------
  public void saveToFile() {
//    InterpreterInfoSaving info = new InterpreterInfoSaving();
//    info.interpreterSettings = Maps.newHashMap(interpreterSettings);
//    info.interpreterRepositories = interpreterRepositories;
//
//    File file = new File(conf.getConfigFSDir() + FILENAME);
//    boolean isFileCreated = file.getParentFile().mkdirs();
//    LOGGER.info("Interpreter settings file {} created - {}", file.getParent(), isFileCreated);
//    try {
//      File tempFile = File.createTempFile(file.getName(), null, file.getParentFile());
//      try (FileOutputStream out = new FileOutputStream(tempFile)) {
//        IOUtils.write(new Gson().toJson(info), out, "UTF-16");
//        Files.copy(tempFile.toPath(), file.toPath());
//      } catch (IOException e) {
//        LOGGER.error("Error saving interpreter settings", e);
//      } finally {
//        Files.delete(tempFile.toPath());
//      }
//    } catch (IOException e) {
//      LOGGER.error("Error saving interpreter settings", e);
//    }
  }

  //----------------------------- UTIL -----------------------------
  //FIXME
  /**
   * Converts {@link InterpreterSettingV2#properties} from {@code Properties}
   * or {@code <String, InterpreterProperty>} to {@code <String, InterpreterPropertyOld>}
   *
   * @return copy of {@link InterpreterSettingV2#properties} converted to
   * {@code Map<String, InterpreterPropertyOld>}
   */
  private Map<String, InterpreterPropertyOld> convertInterpreterProperties(Object properties) {
    if (properties instanceof Properties) {
      Map<String, InterpreterPropertyOld> newProperties = new HashMap<>();
      Properties p = (Properties) properties;
      for (String key : p.stringPropertyNames()) {
        Object entry = p.get(key);
        if (!(entry instanceof Properties)) {
          InterpreterPropertyOld newProperty = new InterpreterPropertyOld(
              key,
              entry,
              InterpreterPropertyType.STRING.getValue());
          newProperties.put(key, newProperty);
        } else {
          // already converted
          return new HashMap<>((Map<String, InterpreterPropertyOld>) properties);
        }
      }
      return newProperties;

    } else if (properties instanceof Map) {
      Map<String, Object> dProperties =
          (Map<String, Object>) properties;
      Map<String, InterpreterPropertyOld> newProperties = new HashMap<>();
      for (String key : dProperties.keySet()) {
        Object value = dProperties.get(key);
        if (value instanceof InterpreterPropertyOld) {
          return new HashMap<>((Map<String, InterpreterPropertyOld>) properties);
        } else if (value instanceof Properties) {
          Properties stringMap = (Properties) value;
          InterpreterPropertyOld newProperty = new InterpreterPropertyOld(
              key,
              stringMap.get("value"),
              stringMap.containsKey("type") ? stringMap.get("type").toString() : "string");

          newProperties.put(newProperty.getName(), newProperty);
        } else if (value instanceof InterpreterProperty){
          InterpreterProperty dProperty = (InterpreterProperty) value;
          InterpreterPropertyOld property = new InterpreterPropertyOld(
              key,
              dProperty.getValue(),
              dProperty.getType() != null ? dProperty.getType() : "string"
              // in case user forget to specify type in interpreter-setting.json
          );
          newProperties.put(key, property);
        } else if (value instanceof String) {
          InterpreterPropertyOld newProperty = new InterpreterPropertyOld(
              key,
              value,
              "string");

          newProperties.put(newProperty.getName(), newProperty);
        } else {
          throw new RuntimeException("Can not convert this type of property: " +
              value.getClass());
        }
      }
      return newProperties;
    }
    throw new RuntimeException("Can not convert this type: " + properties.getClass());
  }
}
