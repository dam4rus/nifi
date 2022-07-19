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

package org.apache.nifi.processors.airtable.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.airtable.model.v0.AirtableRecord;
import okhttp3.Request;
import org.apache.nifi.processors.airtable.model.v0.AirtableRecords;

public class AirtableRestService {

    private final String V0_BASE_URL = "https://api.airtable.com";

    private final ApiVersion apiVersion;
    private final String apiToken;
    private final String baseId;
    private final String tableId;
    private final OkHttpClient httpClient;

    public AirtableRestService(final ApiVersion apiVersion, final String apiToken, final String baseId, final String tableId) {
        this.apiVersion = apiVersion;
        this.apiToken = apiToken;
        this.baseId = baseId;
        this.tableId = tableId;
        this.httpClient = new OkHttpClient();
    }

    public Collection<AirtableRecord> getRecords(final String filterModifiedAfter) {
        final String url = V0_BASE_URL + "/" + apiVersion.value() + "/" + baseId + "/" + tableId;
        final HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(url)).newBuilder();
        if (filterModifiedAfter != null) {
            final String filterByFormula = "IS_AFTER(LAST_MODIFIED_TIME(), DATETIME_PARSE(\"" + filterModifiedAfter + "\"))";
            urlBuilder.addQueryParameter("filterByFormula", filterByFormula);
        }
        final Request request = new Request.Builder()
                .url(urlBuilder.build())
                .addHeader("Authorization", "Bearer " + apiToken)
                .get()
                .build();

        try (final Response response = httpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                final StringBuilder exceptionMessageBuilder = new StringBuilder("Invalid response" +
                        " Code: " + response.code() +
                        " Message: " + response.message());
                if (response.body() != null) {
                    exceptionMessageBuilder.append(" Body: ").append(response.body().string());
                }
                throw new ProcessException(exceptionMessageBuilder.toString());
            }

            final ObjectMapper objectMapper = new ObjectMapper();
            final AirtableRecords records = objectMapper.readValue(Objects.requireNonNull(response.body()).byteStream(), AirtableRecords.class);
            return records.getRecords();
        } catch (final IOException e) {
            throw new ProcessException(String.format("Airtable HTTP request failed [%s]", request.url()), e);
        }
    }
}
