/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.services.azure;

import java.util.stream.Stream;
import org.apache.nifi.components.DescribedValue;

public enum CredentialCofigurationStrategy implements DescribedValue {
    DEFAULT_CREDENTIAL("default-credential", "Default Credential", "Uses default credential chain. It first checks environment variables, before trying managed identity."),
    MANAGED_IDENTITY("managed-identity", "Managed Identity", "Azure Virtual Machine Managed Identity (it can only be used when NiFi is running on Azure)."),
    SERVICE_PRINCIPAL("service-principal", "Service Principal", "Use Service Principal.");

    final private String value;
    final private String displayName;
    final private String description;

    CredentialCofigurationStrategy(String value, String displayName, String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

//    public static CredentialCofigurationStrategy forName(String name) {
//        return Stream.of(values()).filter(credentialCofigurationStrategy -> credentialCofigurationStrategy.getValue().equalsIgnoreCase(name))
//                .findFirst()
//                .orElseThrow(() -> new IllegalArgumentException("Invalid CredentialCofigurationStrategy: " + name));
//    }
}
