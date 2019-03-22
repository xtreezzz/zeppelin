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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.RepositoryException;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.collection.CollectRequest;
import org.sonatype.aether.graph.Dependency;
import org.sonatype.aether.graph.DependencyFilter;
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
//TODO(egorklimov) @Component?
public class DependencyResolver extends AbstractDependencyResolver {
  private final Logger logger = LoggerFactory.getLogger(DependencyResolver.class);

  private final String[] exclusions = new String[0];

  public DependencyResolver(final String localRepoPath) {
    super(localRepoPath);
  }

  public List<File> load(final String artifact) throws RepositoryException, IOException {
    return load(artifact, new LinkedList<String>());
  }

  public synchronized List<File> load(final String artifact, final Collection<String> excludes)
      throws RepositoryException, IOException {
    if (StringUtils.isBlank(artifact)) {
      // Skip dependency loading if artifact is empty
      return new LinkedList<>();
    }

    // <groupId>:<artifactId>[:<extension>[:<classifier>]]:<version>
    final int numSplits = artifact.split(":").length;
    if (numSplits >= 3 && numSplits <= 6) {
      return loadFromMvn(artifact, excludes);
    } else {
      final LinkedList<File> libs = new LinkedList<>();
      libs.add(new File(artifact));
      return libs;
    }
  }

  public List<File> load(final String artifact, final File destPath) throws IOException, RepositoryException {
    return load(artifact, new LinkedList<String>(), destPath);
  }

  public List<File> load(final String artifact, final Collection<String> excludes, final File destPath) throws RepositoryException, IOException {
    List<File> libs = new LinkedList<>();

    if (StringUtils.isNotBlank(artifact)) {
      libs = load(artifact, excludes);

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

  public synchronized void copyLocalDependency(final String srcPath, final File destPath)
      throws IOException {
    if (StringUtils.isBlank(srcPath)) {
      return;
    }

    final File srcFile = new File(srcPath);
    final File destFile = new File(destPath, srcFile.getName());

    if (!destFile.exists() || !FileUtils.contentEquals(srcFile, destFile)) {
      FileUtils.copyFile(srcFile, destFile);
      logger.debug("copy {} to {}", srcFile.getAbsolutePath(), destPath);
    }
  }

  private List<File> loadFromMvn(final String artifact, final Collection<String> excludes)
      throws RepositoryException {
    final Collection<String> allExclusions = new LinkedList<>();
    allExclusions.addAll(excludes);
    allExclusions.addAll(Arrays.asList(exclusions));

    final List<ArtifactResult> listOfArtifact;
    listOfArtifact = getArtifactsWithDep(artifact, allExclusions);

    final Iterator<ArtifactResult> it = listOfArtifact.iterator();
    while (it.hasNext()) {
      final Artifact a = it.next().getArtifact();
      final String gav = a.getGroupId() + ":" + a.getArtifactId() + ":" + a.getVersion();
      for (final String exclude : allExclusions) {
        if (gav.startsWith(exclude)) {
          it.remove();
          break;
        }
      }
    }

    final List<File> files = new LinkedList<>();
    for (final ArtifactResult artifactResult : listOfArtifact) {
      files.add(artifactResult.getArtifact().getFile());
      logger.debug("load {}", artifactResult.getArtifact().getFile().getAbsolutePath());
    }

    return files;
  }

  /**
   * @param dependency
   * @param excludes list of pattern can either be of the form groupId:artifactId
   * @return
   * @throws Exception
   */
  @Override
  public List<ArtifactResult> getArtifactsWithDep(final String dependency,
      final Collection<String> excludes)
      throws RepositoryException {
    final Artifact artifact = new DefaultArtifact(dependency);
    final DependencyFilter classpathFilter = DependencyFilterUtils.classpathFilter(JavaScopes.COMPILE);
    final PatternExclusionsDependencyFilter exclusionFilter =
        new PatternExclusionsDependencyFilter(excludes);

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
      return system.resolveDependencies(session, dependencyRequest).getArtifactResults();
    } catch (final NullPointerException | DependencyResolutionException ex) {
      throw new RepositoryException(
          String.format("Cannot fetch dependencies for %s", dependency), ex);
    }
  }
}
