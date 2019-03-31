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

package org.apache.zeppelin;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.repository.internal.MavenRepositorySystemSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.RepositoryException;
import org.sonatype.aether.RepositorySystem;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.collection.CollectRequest;
import org.sonatype.aether.graph.Dependency;
import org.sonatype.aether.graph.DependencyFilter;
import org.sonatype.aether.repository.LocalRepository;
import org.sonatype.aether.repository.RemoteRepository;
import org.sonatype.aether.resolution.ArtifactResult;
import org.sonatype.aether.resolution.DependencyRequest;
import org.sonatype.aether.resolution.DependencyResolutionException;
import org.sonatype.aether.util.artifact.DefaultArtifact;
import org.sonatype.aether.util.artifact.JavaScopes;
import org.sonatype.aether.util.filter.DependencyFilterUtils;
import org.sonatype.aether.util.filter.PatternExclusionsDependencyFilter;


/**
 * Deps resolver.
 * Add new dependencies from mvn repository (at runtime) to Zeppelin.
 */
public class DependencyResolver {
  private final Logger logger = LoggerFactory.getLogger(DependencyResolver.class);

  protected RepositorySystem system = RepositorySystemFactory.newRepositorySystem();
  protected final List<RemoteRepository> repos = new LinkedList<>();
  protected MavenRepositorySystemSession session;

  public DependencyResolver(final List<Repository> remoteRepositories) {
    session = new MavenRepositorySystemSession();
    final LocalRepository localRepo = new LocalRepository(System.getProperty("user.home") + "/.m2/repository");
    session.setLocalRepositoryManager(system.newLocalRepositoryManager(localRepo));
    for (final Repository repository : remoteRepositories) {
      repos.add(new RemoteRepository("central", "default", repository.getUrl()));
    }
  }

  public List<File> load(final String artifact, final File destPath) throws RepositoryException, IOException {
    List<File> libs = new LinkedList<>();

    if (StringUtils.isNotBlank(artifact)) {
      libs = loadFromMvn(artifact);

      for (final File srcFile : libs) {
        final File destFile = new File(destPath, srcFile.getName());
        if (!destFile.exists() || !FileUtils.contentEquals(srcFile, destFile)) {
          FileUtils.copyFile(srcFile, destFile);
          logger.debug("copy {} to {}", srcFile.getAbsolutePath(), destPath);
        }
      }
    }
    return libs;
  }

  private List<File> loadFromMvn(final String artifactName) throws RepositoryException {

    final List<ArtifactResult> listOfArtifact;
    final Artifact artifact = new DefaultArtifact(artifactName);
    final DependencyFilter classpathFilter = DependencyFilterUtils.classpathFilter(JavaScopes.COMPILE);
    final PatternExclusionsDependencyFilter exclusionFilter = new PatternExclusionsDependencyFilter();

    final CollectRequest collectRequest = new CollectRequest();
    collectRequest.setRoot(new Dependency(artifact, JavaScopes.COMPILE));

    synchronized (repos) {
      for (final RemoteRepository repo : repos) {
        collectRequest.addRepository(repo);
      }
    }
    final DependencyRequest dependencyRequest = new DependencyRequest(collectRequest,
            DependencyFilterUtils.andFilter(exclusionFilter, classpathFilter));
    try {
      listOfArtifact =  system.resolveDependencies(session, dependencyRequest).getArtifactResults();
    } catch (final NullPointerException | DependencyResolutionException ex) {
      throw new RepositoryException(
              String.format("Cannot fetch dependencies for %s", artifactName), ex);
    }

    final List<File> files = new LinkedList<>();
    for (final ArtifactResult artifactResult : listOfArtifact) {
      files.add(artifactResult.getArtifact().getFile());
      logger.debug("load {}", artifactResult.getArtifact().getFile().getAbsolutePath());
    }

    return files;
  }

  public static RemoteRepository newCentralRepository() {
    return new RemoteRepository("central", "default", "http://repo1.maven.org/maven2/");
  }

  public static RemoteRepository newLocalRepository() {
    return new RemoteRepository("local", "default", "file://" + System.getProperty("user.home") + "/.m2/repository");
  }
}
