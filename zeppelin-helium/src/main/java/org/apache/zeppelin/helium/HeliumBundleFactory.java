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
package org.apache.zeppelin.helium;

import com.github.eirslett.maven.plugins.frontend.lib.*;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.helium.V2.HeliumRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/**
 * Load helium visualization & spell
 */
public class HeliumBundleFactory {
  private Logger logger = LoggerFactory.getLogger(HeliumBundleFactory.class);
  private static final String NODE_VERSION = "v6.9.1";
  private static final String NPM_VERSION = "3.10.8";
  private static final String YARN_VERSION = "v0.21.3";
  private static final String NPM_PACKAGE_NAME = "npm";
  protected static final String HELIUM_LOCAL_REPO = "helium-bundle";
  private static final String HELIUM_BUNDLES_DIR = "bundles";
  private static final String HELIUM_LOCAL_MODULE_DIR = "local_modules";
  private static final String HELIUM_BUNDLES_SRC_DIR = "src";
  private static final String HELIUM_BUNDLES_SRC = "load.js";
  private static final String YARN_CACHE_DIR = "yarn-cache";
  private static final String PACKAGE_JSON = "package.json";
  private static final String HELIUM_BUNDLE_CACHE = "helium.bundle.cache.js";
  private static final String HELIUM_BUNDLE = "helium.bundle.js";
  private static final String HELIUM_BUNDLES_VAR = "heliumBundles";
  private static final int FETCH_RETRY_COUNT = 2;
  private static final int FETCH_RETRY_FACTOR_COUNT = 1;
  private static final int FETCH_RETRY_MIN_TIMEOUT = 5000; // Milliseconds

  private final FrontendPluginFactory frontEndPluginFactory;
  private final File nodeInstallationDirectory;
  private final File heliumLocalRepoDirectory;
  private final File heliumBundleDirectory;
  private final File heliumLocalModuleDirectory;
  private final File yarnCacheDir;
  private String defaultNodeInstallerUrl;
  private String defaultNpmInstallerUrl;
  private String defaultYarnInstallerUrl;
  private Gson gson;
  private boolean nodeAndNpmInstalled = false;

  private ByteArrayOutputStream out = new ByteArrayOutputStream();

  public HeliumBundleFactory() {
    this.heliumLocalRepoDirectory = new File("helium-repo", HELIUM_LOCAL_REPO);
    this.heliumBundleDirectory = new File(heliumLocalRepoDirectory, HELIUM_BUNDLES_DIR);
    this.heliumLocalModuleDirectory = new File(heliumLocalRepoDirectory, HELIUM_LOCAL_MODULE_DIR);
    this.yarnCacheDir = new File(heliumLocalRepoDirectory, YARN_CACHE_DIR);
    this.defaultNodeInstallerUrl = "https://nodejs.org/dist/";
    this.defaultNpmInstallerUrl = "http://registry.npmjs.org/";
    this.defaultYarnInstallerUrl = "https://github.com/yarnpkg/yarn/releases/download/";
    this.nodeInstallationDirectory = this.heliumLocalRepoDirectory;

    this.frontEndPluginFactory =
            new FrontendPluginFactory(heliumLocalRepoDirectory, nodeInstallationDirectory);

    this.gson = new Gson();
  }

  void installNodeAndNpm() throws TaskRunnerException {
    if (nodeAndNpmInstalled) {
      return;
    }
    try {
      NodeInstaller nodeInstaller = frontEndPluginFactory
              .getNodeInstaller(getProxyConfig(isSecure(defaultNodeInstallerUrl)));
      nodeInstaller.setNodeVersion(NODE_VERSION);
      nodeInstaller.setNodeDownloadRoot(defaultNodeInstallerUrl);
      nodeInstaller.install();

      NPMInstaller npmInstaller = frontEndPluginFactory
              .getNPMInstaller(getProxyConfig(isSecure(defaultNpmInstallerUrl)));
      npmInstaller.setNpmVersion(NPM_VERSION);
      npmInstaller.setNpmDownloadRoot(defaultNpmInstallerUrl + "/" + NPM_PACKAGE_NAME + "/-/");
      npmInstaller.install();

      YarnInstaller yarnInstaller = frontEndPluginFactory
              .getYarnInstaller(getProxyConfig(isSecure(defaultYarnInstallerUrl)));
      yarnInstaller.setYarnVersion(YARN_VERSION);
      yarnInstaller.setYarnDownloadRoot(defaultYarnInstallerUrl);
      yarnInstaller.install();
      yarnCacheDir.mkdirs();
      String yarnCacheDirPath = yarnCacheDir.getAbsolutePath();
      yarnCommand(frontEndPluginFactory, "config set cache-folder " + yarnCacheDirPath);

      configureLogger();
      nodeAndNpmInstalled = true;
    } catch (InstallationException e) {
      logger.error(e.getMessage(), e);
    }
  }

  private ProxyConfig getProxyConfig(boolean isSecure) {
    List<ProxyConfig.Proxy> proxies = new LinkedList<>();

    String httpProxy = StringUtils.isBlank(System.getenv("http_proxy")) ?
            System.getenv("HTTP_PROXY") : System.getenv("http_proxy");

    String httpsProxy = StringUtils.isBlank(System.getenv("https_proxy")) ?
            System.getenv("HTTPS_PROXY") : System.getenv("https_proxy");

    try {
      if (isSecure && StringUtils.isNotBlank(httpsProxy))
        proxies.add(generateProxy("secure", new URI(httpsProxy)));
      else if (!isSecure && StringUtils.isNotBlank(httpProxy))
        proxies.add(generateProxy("insecure", new URI(httpProxy)));
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
    return new ProxyConfig(proxies);
  }

  private ProxyConfig.Proxy generateProxy(String proxyId, URI uri) {

    String protocol = uri.getScheme();
    String host = uri.getHost();
    int port = uri.getPort() <= 0 ? 80 : uri.getPort();

    String username = null, password = null;
    if (uri.getUserInfo() != null) {
      String[] authority = uri.getUserInfo().split(":");
      if (authority.length == 2) {
        username = authority[0];
        password = authority[1];
      } else if (authority.length == 1) {
        username = authority[0];
      }
    }
    String nonProxyHosts = StringUtils.isBlank(System.getenv("no_proxy")) ?
            System.getenv("NO_PROXY") : System.getenv("no_proxy");
    return new ProxyConfig.Proxy(proxyId, protocol, host, port, username, password, nonProxyHosts);
  }

  private boolean isSecure(String url) {
    return url.toLowerCase().startsWith("https");
  }

  public void buildAllPackages(List<HeliumRegistry.HeliumPackage> pkgs) throws IOException {
    buildAllPackages(pkgs, false);
  }

  private File getHeliumPackageDirectory(String pkgName) {
    return new File(heliumBundleDirectory, pkgName);
  }

  private File getHeliumPackageSourceDirectory(String pkgName) {
    return new File(heliumBundleDirectory, pkgName + "/" + HELIUM_BUNDLES_SRC_DIR);
  }

  private File getHeliumPackageBundleCache(String pkgName) {
    return new File(heliumBundleDirectory, pkgName + "/" + HELIUM_BUNDLE_CACHE);
  }

  private static List<String> unTgz(File tarFile, File directory) throws IOException {
    List<String> result = new ArrayList<>();
    try (TarArchiveInputStream in = new TarArchiveInputStream(
            new GzipCompressorInputStream(new FileInputStream(tarFile)))) {
      TarArchiveEntry entry = in.getNextTarEntry();
      while (entry != null) {
        if (entry.isDirectory()) {
          entry = in.getNextTarEntry();
          continue;
        }
        File curfile = new File(directory, entry.getName());
        File parent = curfile.getParentFile();
        if (!parent.exists()) {
          parent.mkdirs();
        }
        try (OutputStream out = new FileOutputStream(curfile)) {
          IOUtils.copy(in, out);
        }
        result.add(entry.getName());
        entry = in.getNextTarEntry();
      }
    }
    return result;
  }

  /**
   * @return main file name of this helium package (relative path)
   */
  private String downloadPackage(HeliumRegistry.HeliumPackage pkg, String[] nameAndVersion, File bundleDir,
                                 String templateWebpackConfig, String templatePackageJson,
                                 FrontendPluginFactory fpf) throws IOException, TaskRunnerException {
    if (bundleDir.exists()) {
      FileUtils.deleteQuietly(bundleDir);
    }
    FileUtils.forceMkdir(bundleDir);

    FileFilter copyFilter = new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        String fileName = pathname.getName();
        if (fileName.startsWith(".") || fileName.startsWith("#") || fileName.startsWith("~")) {
          return false;
        } else {
          return true;
        }
      }
    };

    if (isLocalPackage(pkg)) {
      FileUtils.copyDirectory(
              new File(pkg.getArtifact()),
              bundleDir,
              copyFilter);
    } else {
      // if online package
      String version = nameAndVersion[1];
      File tgz = new File(heliumLocalRepoDirectory, pkg.getName() + "-" + version + ".tgz");
      tgz.delete();

      // wget, extract and move dir to `bundles/${pkg.getName()}`, and remove tgz
      npmCommand(fpf, "pack " + pkg.getArtifact());
      File extracted = new File(heliumBundleDirectory, "package");
      FileUtils.deleteDirectory(extracted);
      List<String> entries = unTgz(tgz, heliumBundleDirectory);
      for (String entry : entries) logger.debug("Extracted " + entry);
      tgz.delete();
      FileUtils.copyDirectory(extracted, bundleDir);
      FileUtils.deleteDirectory(extracted);
    }

    // 1. setup package.json
    File existingPackageJson = new File(bundleDir, "package.json");
    JsonReader reader = new JsonReader(new FileReader(existingPackageJson));
    Map<String, Object> packageJson = gson.fromJson(reader,
            new TypeToken<Map<String, Object>>() {
            }.getType());
    Map<String, String> existingDeps = (Map<String, String>) packageJson.get("dependencies");
    String mainFileName = (String) packageJson.get("main");

    StringBuilder dependencies = new StringBuilder();
    int index = 0;
    for (Map.Entry<String, String> e : existingDeps.entrySet()) {
      dependencies.append("    \"").append(e.getKey()).append("\": ");
      if (e.getKey().equals("zeppelin-vis") ||
              e.getKey().equals("zeppelin-tabledata") ||
              e.getKey().equals("zeppelin-spell")) {
        dependencies.append("\"file:../../" + HELIUM_LOCAL_MODULE_DIR + "/")
                .append(e.getKey()).append("\"");
      } else {
        dependencies.append("\"").append(e.getValue()).append("\"");
      }

      if (index < existingDeps.size() - 1) {
        dependencies.append(",\n");
      }
      index = index + 1;
    }

    FileUtils.deleteQuietly(new File(bundleDir, PACKAGE_JSON));
    templatePackageJson = templatePackageJson.replaceFirst("PACKAGE_NAME", pkg.getName());
    templatePackageJson = templatePackageJson.replaceFirst("MAIN_FILE", mainFileName);
    templatePackageJson = templatePackageJson.replaceFirst("DEPENDENCIES", dependencies.toString());
    FileUtils.write(new File(bundleDir, PACKAGE_JSON), templatePackageJson);

    // 2. setup webpack.config
    FileUtils.write(new File(bundleDir, "webpack.config.js"), templateWebpackConfig);

    return mainFileName;
  }

  private void prepareSource(HeliumRegistry.HeliumPackage pkg, String[] moduleNameVersion,
                             String mainFileName) throws IOException {
    StringBuilder loadJsImport = new StringBuilder();
    StringBuilder loadJsRegister = new StringBuilder();
    String className = "bundles" + pkg.getName().replaceAll("[-_]", "");

    // remove postfix `.js` for ES6 import
    if (mainFileName.endsWith(".js")) {
      mainFileName = mainFileName.substring(0, mainFileName.length() - 3);
    }

    loadJsImport
            .append("import ")
            .append(className)
            .append(" from \"../" + mainFileName + "\"\n");

    loadJsRegister.append(HELIUM_BUNDLES_VAR + ".push({\n");
    loadJsRegister.append("id: \"" + moduleNameVersion[0] + "\",\n");
    loadJsRegister.append("name: \"" + pkg.getName() + "\",\n");
    loadJsRegister.append("icon: " + gson.toJson(pkg.getIcon()) + ",\n");
    loadJsRegister.append("type: \"" + pkg.getType() + "\",\n");
    loadJsRegister.append("class: " + className + "\n");
    loadJsRegister.append("})\n");

    File srcDir = getHeliumPackageSourceDirectory(pkg.getName());
    FileUtils.forceMkdir(srcDir);
    FileUtils.write(new File(srcDir, HELIUM_BUNDLES_SRC),
            loadJsImport.append(loadJsRegister).toString());
  }

  private synchronized void installNodeModules(FrontendPluginFactory fpf) throws IOException {
    try {
      out.reset();
      String commandForNpmInstall =
              String.format("install --fetch-retries=%d --fetch-retry-factor=%d " +
                              "--fetch-retry-mintimeout=%d",
                      FETCH_RETRY_COUNT, FETCH_RETRY_FACTOR_COUNT, FETCH_RETRY_MIN_TIMEOUT);
      logger.info("Installing required node modules");
      yarnCommand(fpf, commandForNpmInstall);
      logger.info("Installed required node modules");
    } catch (TaskRunnerException e) {
      throw new IOException(e);
    }
  }

  private synchronized File bundleHeliumPackage(FrontendPluginFactory fpf,
                                                File bundleDir) throws IOException {
    try {
      out.reset();
      logger.info("Bundling helium packages");
      yarnCommand(fpf, "run bundle");
      logger.info("Bundled helium packages");
    } catch (TaskRunnerException e) {
      throw new IOException(new String(out.toByteArray()));
    }

    File heliumBundle = new File(bundleDir, HELIUM_BUNDLE);
    if (!heliumBundle.isFile()) {
      throw new IOException(
              "Can't create bundle: ");
    }


    return heliumBundle;
  }

  public synchronized File buildPackage(HeliumRegistry.HeliumPackage pkg,
                                        boolean rebuild,
                                        boolean recopyLocalModule) throws URISyntaxException, IOException {
    if (pkg == null) {
      return null;
    }

    String[] moduleNameVersion = getNpmModuleNameAndVersion(pkg);
    if (moduleNameVersion == null) {
      logger.warn("Can't get module name and version of package " + pkg.getName());
      return null;
    }

    String pkgName = pkg.getName();
    File bundleDir = getHeliumPackageDirectory(pkgName);
    File bundleCache = getHeliumPackageBundleCache(pkgName);

    synchronized (this) {
      if (!rebuild && bundleCache.exists() && !bundleCache.isDirectory()) {
        return bundleCache;
      }

      // 0. install node, npm (should be called before `downloadPackage`
      try {
        installNodeAndNpm();
      } catch (TaskRunnerException e) {
        throw new IOException(e);
      }

      // 1. prepare directories
      if (!heliumLocalRepoDirectory.exists() || !heliumLocalRepoDirectory.isDirectory()) {
        FileUtils.deleteQuietly(heliumLocalRepoDirectory);
        FileUtils.forceMkdir(heliumLocalRepoDirectory);
      }
      FrontendPluginFactory fpf = new FrontendPluginFactory(
              bundleDir, nodeInstallationDirectory);

      // resources: webpack.js, package.json
      String templateWebpackConfig = Resources.toString(
              Resources.getResource("helium/webpack.config.js"), Charsets.UTF_8);
      String templatePackageJson = Resources.toString(
              Resources.getResource("helium/" + PACKAGE_JSON), Charsets.UTF_8);

      // 2. download helium package using `npm pack`
      String mainFileName = null;
      try {
        mainFileName = downloadPackage(pkg, moduleNameVersion, bundleDir,
                templateWebpackConfig, templatePackageJson, fpf);
      } catch (TaskRunnerException e) {
        throw new IOException(e);
      }

      // 3. prepare bundle source
      prepareSource(pkg, moduleNameVersion, mainFileName);

      // 4. install node and local modules for a bundle
      copyFrameworkModulesToInstallPath(recopyLocalModule); // should copy local modules first
      installNodeModules(fpf);

      // 5. let's bundle and update cache
      File heliumBundle = bundleHeliumPackage(fpf, bundleDir);
      bundleCache.delete();
      FileUtils.moveFile(heliumBundle, bundleCache);

      return bundleCache;
    }
  }

  private synchronized void buildAllPackages(List<HeliumRegistry.HeliumPackage> pkgs, boolean rebuild)
          throws IOException {

    if (pkgs == null || pkgs.size() == 0) {
      return;
    }

    // DON't recopy local modules when build all packages to avoid duplicated copies.
    boolean recopyLocalModules = false;

    for (HeliumRegistry.HeliumPackage pkg : pkgs) {
      try {
        buildPackage(pkg, rebuild, recopyLocalModules);
      } catch (Exception e) {
        logger.error("Failed to build helium package: " + pkg.getArtifact(), e);
      }
    }
  }

  private void copyFrameworkModule(boolean recopy, FileFilter filter, List<String> files, File dest) throws URISyntaxException, IOException {
    if (recopy && dest.exists()) {
      FileUtils.deleteDirectory(dest);
    }

    if (!dest.exists()) {
      for (final String src : files) {
        final byte[] bytes = IOUtils.toByteArray(getClass().getClassLoader().getResourceAsStream(src));
        FileUtils.writeByteArrayToFile(new File(dest, src.substring(src.lastIndexOf('/') + 1)), bytes);
      }
    }
  }

  private void deleteYarnCache() {
    FilenameFilter filter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if ((name.startsWith("npm-zeppelin-vis-") ||
                name.startsWith("npm-zeppelin-tabledata-") ||
                name.startsWith("npm-zeppelin-spell-")) &&
                dir.isDirectory()) {
          return true;
        }

        return false;
      }
    };

    File[] localModuleCaches = yarnCacheDir.listFiles(filter);
    if (localModuleCaches != null) {
      for (File f : localModuleCaches) {
        FileUtils.deleteQuietly(f);
      }
    }
  }

  void copyFrameworkModulesToInstallPath(boolean recopy)
          throws URISyntaxException, IOException {

    FileFilter npmPackageCopyFilter = new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        String fileName = pathname.getName();
        if (fileName.startsWith(".") || fileName.startsWith("#") || fileName.startsWith("~")) {
          return false;
        } else {
          return true;
        }
      }
    };

    FileUtils.forceMkdir(heliumLocalModuleDirectory);
    // should delete yarn caches for local modules since they might be updated
    deleteYarnCache();

    // install tabledata module
    File tabledataModuleInstallPath = new File(heliumLocalModuleDirectory, "zeppelin-tabledata");
    final List<String> tabledataFiles = new ArrayList<>();
    tabledataFiles.add("tabledata/.npmignore");
    tabledataFiles.add("tabledata/networkdata.js");
    tabledataFiles.add("tabledata/columnselector.js");
    tabledataFiles.add("tabledata/pivot_settings.html");
    tabledataFiles.add("tabledata/transformation.js");
    tabledataFiles.add("tabledata/tabledata.js");
    tabledataFiles.add("tabledata/network_settings.html");
    tabledataFiles.add("tabledata/networkdata.test.js");
    tabledataFiles.add("tabledata/passthrough.js");
    tabledataFiles.add("tabledata/tabledata.test.js");
    tabledataFiles.add("tabledata/advanced-transformation-util.test.js");
    tabledataFiles.add("tabledata/package.json");
    tabledataFiles.add("tabledata/advanced-transformation.js");
    tabledataFiles.add("tabledata/advanced-transformation-setting.html");
    tabledataFiles.add("tabledata/pivot.js");
    tabledataFiles.add("tabledata/dataset.js");
    tabledataFiles.add("tabledata/network.js");
    tabledataFiles.add("tabledata/datasetfactory.js");
    tabledataFiles.add("tabledata/advanced-transformation-util.js");
    tabledataFiles.add("tabledata/datasetfactory.test.js");
    tabledataFiles.add("tabledata/columnselector_settings.html");
    copyFrameworkModule(recopy, npmPackageCopyFilter, tabledataFiles, tabledataModuleInstallPath);


    // install visualization module
    File visModuleInstallPath = new File(heliumLocalModuleDirectory, "zeppelin-vis");
    final List<String> visualizationFiles = new ArrayList<>();
    visualizationFiles.add("visualization/.npmignore");
    visualizationFiles.add("visualization/package.json");
    visualizationFiles.add("visualization/visualization.js");
    copyFrameworkModule(recopy, npmPackageCopyFilter, visualizationFiles, visModuleInstallPath);

    File visBuiltinsModuleInstallPath = new File(visModuleInstallPath, "builtins");
    final List<String> visualizationBuiltinsFiles = new ArrayList<>();
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-areachart.js");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-barchart.js");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-d3network.js");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-displayXAxis.html");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-linechart.js");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-nvd3chart.js");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-piechart.js");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-scatterchart.js");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-table.js");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-table-grid-filter.html");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-table-setting.html");
    visualizationBuiltinsFiles.add("visualization/builtins/visualization-util.js");
    copyFrameworkModule(recopy, npmPackageCopyFilter, visualizationBuiltinsFiles, visBuiltinsModuleInstallPath);

    // install spell module
    File spellModuleInstallPath = new File(heliumLocalModuleDirectory, "zeppelin-spell");
    final List<String> spellFiles = new ArrayList<>();
    spellFiles.add("spell/.npmignore");
    spellFiles.add("spell/index.js");
    spellFiles.add("spell/package.json");
    spellFiles.add("spell/spell-base.js");
    spellFiles.add("spell/spell-result.js");
    copyFrameworkModule(recopy, npmPackageCopyFilter, spellFiles, spellModuleInstallPath);
  }

  private WebpackResult getWebpackResultFromOutput(String output) {
    BufferedReader reader = new BufferedReader(new StringReader(output));

    boolean webpackRunDetected = false;
    boolean resultJsonDetected = false;
    StringBuffer sb = new StringBuffer();
    try {
      String next, line = reader.readLine();
      for (boolean last = (line == null); !last; line = next) {
        last = ((next = reader.readLine()) == null);

        if (!webpackRunDetected) {
          String trimed = line.trim();
          if (trimed.contains("webpack") && trimed.endsWith("--json")) {
            webpackRunDetected = true;
          }
          continue;
        }

        if (!resultJsonDetected) {
          if (line.trim().equals("{")) {
            sb.append(line);
            resultJsonDetected = true;
          }
          continue;
        }

        if (resultJsonDetected && webpackRunDetected) {
          // yarn command always ends with `Done in ... seconds `
          if (!last) {
            sb.append(line);
          }
        }
      }
      return new Gson().fromJson(sb.toString(), WebpackResult.class);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      return new WebpackResult();
    }
  }

  private boolean isLocalPackage(HeliumRegistry.HeliumPackage pkg) {
    return (pkg.getArtifact().startsWith(".") || pkg.getArtifact().startsWith("/"));
  }

  private String[] getNpmModuleNameAndVersion(HeliumRegistry.HeliumPackage pkg) {
    String artifact = pkg.getArtifact();

    //if (isLocalPackage(pkg)) {
    if (false) {
      File packageJson = new File(artifact, "package.json");
      if (!packageJson.isFile()) {
        return null;
      }
      try {
        NpmPackage npmPackage = new Gson().fromJson(FileUtils.readFileToString(packageJson), NpmPackage.class);

        String[] nameVersion = new String[2];
        nameVersion[0] = npmPackage.name;
        nameVersion[1] = npmPackage.version;
        return nameVersion;
      } catch (IOException e) {
        logger.error(e.getMessage(), e);
        return null;
      }
    } else {
      String[] nameVersion = new String[2];

      int pos;
      if ((pos = artifact.indexOf('@')) > 0) {
        nameVersion[0] = artifact.substring(0, pos);
        nameVersion[1] = artifact.substring(pos + 1);
      } else if (
              (pos = artifact.indexOf('^')) > 0 ||
                      (pos = artifact.indexOf('~')) > 0) {
        nameVersion[0] = artifact.substring(0, pos);
        nameVersion[1] = artifact.substring(pos);
      } else {
        nameVersion[0] = artifact;
        nameVersion[1] = "";
      }
      return nameVersion;
    }
  }

  synchronized void install(HeliumRegistry.HeliumPackage pkg) throws TaskRunnerException {
    String commandForNpmInstallArtifact =
            String.format("install %s --fetch-retries=%d --fetch-retry-factor=%d " +
                            "--fetch-retry-mintimeout=%d", pkg.getArtifact(),
                    FETCH_RETRY_COUNT, FETCH_RETRY_FACTOR_COUNT, FETCH_RETRY_MIN_TIMEOUT);
    npmCommand(commandForNpmInstallArtifact);
  }

  private void npmCommand(String args) throws TaskRunnerException {
    npmCommand(args, new HashMap<String, String>());
  }

  private void npmCommand(String args, Map<String, String> env) throws TaskRunnerException {
    NpmRunner npm = frontEndPluginFactory.getNpmRunner(
            getProxyConfig(isSecure(defaultNpmInstallerUrl)), defaultNpmInstallerUrl);
    npm.execute(args, env);
  }

  private void npmCommand(FrontendPluginFactory fpf, String args) throws TaskRunnerException {
    npmCommand(args, new HashMap<String, String>());
  }

  private void yarnCommand(FrontendPluginFactory fpf, String args) throws TaskRunnerException {
    yarnCommand(fpf, args, new HashMap<String, String>());
  }

  private void yarnCommand(FrontendPluginFactory fpf,
                           String args, Map<String, String> env) throws TaskRunnerException {
    YarnRunner yarn = fpf.getYarnRunner(
            getProxyConfig(isSecure(defaultNpmInstallerUrl)), defaultNpmInstallerUrl);
    yarn.execute(args, env);
  }

  private synchronized void configureLogger() {
    org.apache.log4j.Logger npmLogger = org.apache.log4j.Logger.getLogger(
            "com.github.eirslett.maven.plugins.frontend.lib.DefaultYarnRunner");
    Enumeration appenders = org.apache.log4j.Logger.getRootLogger().getAllAppenders();

    if (appenders != null) {
      while (appenders.hasMoreElements()) {
        Appender appender = (Appender) appenders.nextElement();
        appender.addFilter(new Filter() {

          @Override
          public int decide(LoggingEvent loggingEvent) {
            if (loggingEvent.getLoggerName().contains("DefaultYarnRunner")) {
              return Filter.DENY;
            } else {
              return Filter.NEUTRAL;
            }
          }
        });
      }
    }
    npmLogger.addAppender(new WriterAppender(
            new PatternLayout("%m%n"),
            out
    ));
  }


  public void copyFromJar(String source, final Path target) throws URISyntaxException, IOException {
    URI resource = URI.create("jar:" + getClass().getClassLoader().getResource(source).toURI());
    FileSystem fileSystem = FileSystems.newFileSystem(
            resource,
            Collections.<String, String>emptyMap()
    );


    final Path jarPath = fileSystem.getPath(source);

    Files.walkFileTree(jarPath, new SimpleFileVisitor<Path>() {

      private Path currentTarget;

      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        currentTarget = target.resolve(jarPath.relativize(dir).toString());
        Files.createDirectories(currentTarget);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.copy(file, target.resolve(jarPath.relativize(file).toString()), StandardCopyOption.REPLACE_EXISTING);
        return FileVisitResult.CONTINUE;
      }

    });
  }
}
