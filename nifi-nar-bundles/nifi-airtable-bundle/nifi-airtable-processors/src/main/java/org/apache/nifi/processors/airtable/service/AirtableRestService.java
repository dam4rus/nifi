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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.nifi.processor.exception.ProcessException;

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

    public String getRecords(final AirtableGetRecordsFilter filter) {
        final String url = V0_BASE_URL + "/" + apiVersion.value() + "/" + baseId + "/" + tableId;
        final HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(url)).newBuilder();

        if (filter.getFields() != null) {
            for (final String field : filter.getFields()) {
                urlBuilder.addQueryParameter("fields[]", field);
            }
        }

        final List<String> filtersByFormula = new ArrayList<>();
        if (filter.getFilterByFormula() != null) {
            filtersByFormula.add(filter.getFilterByFormula());
        }
        if (filter.getModifiedAfter() != null) {
            final String filterByModifiedTime = "IS_AFTER(LAST_MODIFIED_TIME(),DATETIME_PARSE(\"" + filter.getModifiedAfter() + "\"))";
            filtersByFormula.add(filterByModifiedTime);
        }
        if (filter.getModifiedBefore() != null) {
            final String filterByModifiedTime = "IS_BEFORE(LAST_MODIFIED_TIME(),DATETIME_PARSE(\"" + filter.getModifiedBefore() + "\"))";
            filtersByFormula.add(filterByModifiedTime);
        }
        if (!filtersByFormula.isEmpty()) {
            urlBuilder.addQueryParameter("filterByFormula", "AND(" + String.join(",", filtersByFormula) + ")");
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

            return Objects.requireNonNull(response.body()).string();
        } catch (final IOException e) {
            throw new ProcessException(String.format("Airtable HTTP request failed [%s]", request.url()), e);
        }
    }
}
