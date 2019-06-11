/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.policy.action.data;

import alluxio.AlluxioURI;
import alluxio.Constants;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility methods for the data action.
 */
public final class DataActionUtils {
  private static final Pattern DATA_OPERATION_RE = Pattern.compile(
      "^\\s*(?<location>\\w+)(\\[(?<locationModifier>.+)\\])?\\s*:\\s*"
          + "(?<operation>\\w+)(\\[(?<operationModifier>.+)\\])?\\s*$");

  private DataActionUtils() {} // prevent instantiation

  /**
   * @param uri the UFS URI
   * @return whether the URI is a union UFS
   */
  public static boolean isUnionUfs(AlluxioURI uri) {
    return uri.toString().startsWith(Constants.HEADER_UNION);
  }

  /**
   * Deserializes a body string and returns a list of {@link DataActionDefinition.LocationOperation}.
   *
   * @param body the body string to deserialize
   * @return the list of {@link DataActionDefinition.LocationOperation}
   */
  public static List<DataActionDefinition.LocationOperation> deserializeBody(String body) {
    // the body structure is:
    // body: dataOperation | dataOperation , body
    // dataOperation: location:operation
    // location: locationName | locationName[locationModifier]
    // operation: operationName | operationName[operationModifier]
    body = body.trim();
    if (body.isEmpty()) {
      return Collections.emptyList();
    }

    // split on an empty string will return 1 element array with empty string
    String[] parts = body.split(",");

    List<DataActionDefinition.LocationOperation> dataOperations = new ArrayList<>(parts.length);
    for (String part : parts) {
      Matcher matcher = DATA_OPERATION_RE.matcher(part);
      if (!matcher.matches()) {
        throw new IllegalStateException(
            String.format("Failed to parse %s from body: %s", part, body));
      }
      String locationString = matcher.group("location");
      String operationString = matcher.group("operation");
      String locationModifier = matcher.group("locationModifier");
      String operationModifier = matcher.group("operationModifier");

      locationModifier = (locationModifier != null) ? locationModifier : "";
      operationModifier = (operationModifier != null) ? operationModifier : "";
      DataActionDefinition.Location location =
          DataActionDefinition.Location.valueOf(locationString.toUpperCase());
      DataActionDefinition.Operation operation =
          DataActionDefinition.Operation.valueOf(operationString.toUpperCase());

      dataOperations.add(
          new DataActionDefinition.LocationOperation(location, locationModifier, operation,
              operationModifier));
    }
    return dataOperations;
  }

  /**
   * Encodes a list of {@link DataActionDefinition.LocationOperation} to a string.
   *
   * @param operations the location operation pairs
   * @return the encoded string
   */
  public static String serializeBody(List<DataActionDefinition.LocationOperation> operations) {
    return operations.stream().map(DataActionDefinition.LocationOperation::toString)
        .collect(Collectors.joining(", "));
  }

  /**
   * Creates a URI for a sub UFS inside the union UFS.
   *
   * @param unionUfsUri the union UFS URI
   * @param subUfs the sub UFS alias
   * @return the sub UFS URI in the format of union://sub/path
   */
  public static String createUnionSubUfsUri(AlluxioURI unionUfsUri, String subUfs) {
    try {
      return new URI(unionUfsUri.getScheme(), subUfs, unionUfsUri.getPath(), null, null).toString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
