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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sonatype.aether.transfer.AbstractTransferListener;
import org.sonatype.aether.transfer.TransferEvent;
import org.sonatype.aether.transfer.TransferResource;

/**
 * Simple listener that show deps downloading progress.
 */
public class TransferListener extends AbstractTransferListener {
  private final Logger logger = LoggerFactory.getLogger(TransferListener.class);

  private final Map<TransferResource, Long> downloads = new ConcurrentHashMap<>();

  private int lastLength;

  public TransferListener() {}

  @Override
  public void transferInitiated(final TransferEvent event) {
    final String message =
        event.getRequestType() == TransferEvent.RequestType.PUT ? "Uploading" : "Downloading";

    logger.info(message + ": " + event.getResource().getRepositoryUrl()
                + event.getResource().getResourceName());
  }

  @Override
  public void transferProgressed(final TransferEvent event) {
    final TransferResource resource = event.getResource();
    downloads.put(resource, event.getTransferredBytes());

    final StringBuilder buffer = new StringBuilder(64);

    for (final Map.Entry<TransferResource, Long> entry : downloads.entrySet()) {
      final long total = entry.getKey().getContentLength();
      final long complete = entry.getValue();

      buffer.append(getStatus(complete, total)).append("  ");
    }

    final int pad = lastLength - buffer.length();
    lastLength = buffer.length();
    pad(buffer, pad);
    buffer.append('\r');

    logger.info(buffer.toString());
  }

  private String getStatus(final long complete, final long total) {
    if (total >= 1024) {
      return toKB(complete) + "/" + toKB(total) + " KB ";
    } else if (total >= 0) {
      return complete + "/" + total + " B ";
    } else if (complete >= 1024) {
      return toKB(complete) + " KB ";
    } else {
      return complete + " B ";
    }
  }

  private void pad(final StringBuilder buffer, int spaces) {
    final String block = "                                        ";
    while (spaces > 0) {
      final int n = Math.min(spaces, block.length());
      buffer.append(block, 0, n);
      spaces -= n;
    }
  }

  @Override
  public void transferSucceeded(final TransferEvent event) {
    transferCompleted(event);

    final TransferResource resource = event.getResource();
    final long contentLength = event.getTransferredBytes();
    if (contentLength >= 0) {
      final String type =
          (event.getRequestType() == TransferEvent.RequestType.PUT ? "Uploaded" : "Downloaded");
      final String len = contentLength >= 1024 ? toKB(contentLength) + " KB" : contentLength + " B";

      String throughput = "";
      final long duration = System.currentTimeMillis() - resource.getTransferStartTime();
      if (duration > 0) {
        final DecimalFormat format = new DecimalFormat("0.0", new DecimalFormatSymbols(Locale.ENGLISH));
        final double kbPerSec = (contentLength / 1024.0) / (duration / 1000.0);
        throughput = " at " + format.format(kbPerSec) + " KB/sec";
      }

      logger.info(type + ": " + resource.getRepositoryUrl() + resource.getResourceName() + " ("
          + len + throughput + ")");
    }
  }

  @Override
  public void transferFailed(final TransferEvent event) {
    transferCompleted(event);
    logger.warn("Unsuccessful transfer", event.getException());
  }

  private void transferCompleted(final TransferEvent event) {
    downloads.remove(event.getResource());
    final StringBuilder buffer = new StringBuilder(64);
    pad(buffer, lastLength);
    buffer.append('\r');
    logger.info(buffer.toString());
  }

  @Override
  public void transferCorrupted(final TransferEvent event) {
    logger.error("Corrupted transfer", event.getException());
  }

  private long toKB(final long bytes) {
    return (bytes + 1023) / 1024;
  }

}
