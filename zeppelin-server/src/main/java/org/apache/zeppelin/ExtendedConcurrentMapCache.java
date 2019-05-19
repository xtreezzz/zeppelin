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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.core.serializer.support.SerializationDelegate;
import org.springframework.lang.NonNull;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleInnerConfiguration;
import ru.tinkoff.zeppelin.core.configuration.interpreter.ModuleSource;

/**
 * Extended ConcurrentMapCache with possibility to put/get list of {@link ModuleSource}, {@link ModuleInnerConfiguration},
 * {@link ModuleConfiguration} per value.
 *
 * Needed to expand list of values in methods like {@code getAll()}.
 */
public class ExtendedConcurrentMapCache extends ConcurrentMapCache {

  private static final String GET_ALL_KEY = "GET_ALL";

  public ExtendedConcurrentMapCache(final String name) {
    super(name);
  }

  public ExtendedConcurrentMapCache(final String name,
                                    final boolean allowNullValues) {
    super(name, allowNullValues);
  }

  public ExtendedConcurrentMapCache(final String name,
                                    final ConcurrentMap<Object, Object> store,
                                    final boolean allowNullValues) {
    super(name, store, allowNullValues);
  }

  protected ExtendedConcurrentMapCache(final String name,
                                       final ConcurrentMap<Object, Object> store,
                                       final boolean allowNullValues,
                                       final SerializationDelegate serialization) {
    super(name, store, allowNullValues, serialization);
  }

  /**
   * Returns mapped value.
   * If key equals {@code GET_ALL} it means, that all values should be returned.
   *
   * @param key, key of mapped value or {@code GET_ALL} if all values needed.
   * @return wraped values or {@code null} if there is no value or map is empty.
   */
  @Override
  public ValueWrapper get(@Nonnull final Object key) {
    if (getNativeCache().isEmpty()) {
      return null;
    } else if (key instanceof String && key.equals(GET_ALL_KEY)) {
      return () -> new ArrayList<>(getNativeCache().values());
    }
    return super.get(key);
  }

  /**
   * Puts value by key. If passed values is {@code Iterable} - unpack it and puts each value to the map.
   *
   * @param key The key by which the value will be saved.
   * @param value Saved value, if {@code Iterable} - each value would be saved
   * @see ExtendedConcurrentMapCache#getKey(Object)
   */
  @Override
  public void put(@NonNull final Object key, final Object value) {
    if (value instanceof Iterable) {
      ((Iterable) value).forEach(v -> {
        final Long currentKey = getKey(v);
        if (currentKey != null) {
          super.put(currentKey, v);
        }
      });
    } else {
      super.put(key, value);
    }
  }

  @Override
  public ValueWrapper putIfAbsent(@Nonnull final Object key, final Object value) {
    final List<Entry> entryList = new ArrayList<>();
    if (value instanceof Iterable) {
      ((Iterable) value).forEach(v -> {
        final Long currentKey = getKey(v);
        if (currentKey != null) {
          entryList.add(new AbstractMap.SimpleEntry<>(currentKey, super.putIfAbsent(currentKey, v)));
        }
      });
      return () -> entryList;
    } else {
      return super.putIfAbsent(key, value);
    }
  }

  /**
   * If passed object is {@link ModuleSource} or {@link ModuleConfiguration} or {@link ModuleInnerConfiguration}
   * it would be saved to the map using id.
   *
   * @param value value to store in the map.
   * @return
   */
  private Long getKey(final Object value) {
    Long key = null;
    if (value instanceof ModuleSource) {
      key = ((ModuleSource) value).getId();
    } else if (value instanceof ModuleConfiguration) {
      key = ((ModuleConfiguration) value).getId();
    } else if (value instanceof ModuleInnerConfiguration) {
      key = ((ModuleInnerConfiguration) value).getId();
    }
    return key;
  }
}
