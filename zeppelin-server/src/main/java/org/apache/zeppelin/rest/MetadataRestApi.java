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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.metadata.MetadataNode;
import org.apache.zeppelin.metadata.MetadataServer;
import org.apache.zeppelin.server.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;

/**
 * Metadata rest api.
 */
@Path("/metadata")
@Produces("application/json")
public class MetadataRestApi {

  private static final Logger logger = LoggerFactory.getLogger(MetadataRestApi.class);

  private MetadataServer metadataServer;

  public MetadataRestApi() {
  }

  public MetadataRestApi(MetadataServer metadataServer) {
    this.metadataServer = metadataServer;
  }

  /**
   * Get a metadata by id
   */
  @GET
  @Path("{metadataId}")
  @ZeppelinApi
  public Response getMetadata(@PathParam("metadataId") String metadataId) {
    try {
      MetadataNode metadataNode = metadataServer.getTree(metadataId);
      if (metadataNode == null) {
        return new JsonResponse<>(Response.Status.NOT_FOUND).build();
      } else {
        return new JsonResponse<>(Response.Status.OK, "", metadataNode).build();
      }
    } catch (NullPointerException e) {
      logger.error("Exception in MetadataRestApi", e);
      return new JsonResponse<>(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage(),
          ExceptionUtils.getStackTrace(e)).build();
    }
  }
}
