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
package org.apache.zeppelin.rest;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zeppelin.rest.message.JsonResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.storage.SystemEventDAO;
import ru.tinkoff.zeppelin.storage.SystemEventDTO;
import ru.tinkoff.zeppelin.storage.SystemEventType;

@RestController
@RequestMapping("/api/monitoring")
public class MonitoringRestApi {

  private final SystemEventDAO systemEventDAO;

  public MonitoringRestApi(final SystemEventDAO systemEventDAO) {
    this.systemEventDAO = systemEventDAO;
  }

  @GetMapping(value = "/list", produces = "application/json")
  public ResponseEntity listEvents() {
    try {
      final List<SystemEventDTO> result = new ArrayList<>();
      final List<SystemEvent> events = systemEventDAO.getAllEvents();

      for (final SystemEvent event : events) {
        final SystemEventType type = systemEventDAO.getTypeById(event.getTypeId());
        if (type == null) {
          continue;
        }

        result.add(
            new SystemEventDTO(
                event.getUsername(),
                type.getName().name(),
                event.getMessage(),
                event.getDescription(),
                event.getActionTime()
            )
        );
      }
      return new JsonResponse(HttpStatus.OK, "", result).build();
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(),
              ExceptionUtils.getStackTrace(e)).build();
    }
  }
}
