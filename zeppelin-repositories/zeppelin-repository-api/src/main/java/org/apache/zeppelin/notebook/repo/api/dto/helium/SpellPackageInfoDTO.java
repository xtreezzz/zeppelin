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

package org.apache.zeppelin.notebook.repo.api.dto.helium;

import java.io.Serializable;

/**
 * Immutable Data Transfer Object for SpellPackageInfo.
 */
public final class SpellPackageInfoDTO implements Serializable {
  private final String magic;
  private final String usage;

  public SpellPackageInfoDTO(String magic, String usage) {
    this.magic = magic;
    this.usage = usage;
  }

  public String getMagic() {
    return magic;
  }

  public String getUsage() {
    return usage;
  }
}
