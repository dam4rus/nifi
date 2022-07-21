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

import java.util.ArrayList;
import java.util.List;

public class AirtableGetRecordsFilter {

    private final List<String> fields;
    private final String modifiedAfter;
    private final String modifiedBefore;
    private final String filterByFormula;

    public AirtableGetRecordsFilter() {
        fields = null;
        modifiedAfter = null;
        modifiedBefore = null;
        filterByFormula = null;
    }

    public AirtableGetRecordsFilter(List<String> fields,
            String modifiedAfter,
            String modifiedBefore,
            String filterByFormula) {
        this.fields = fields;
        this.modifiedAfter = modifiedAfter;
        this.modifiedBefore = modifiedBefore;
        this.filterByFormula = filterByFormula;
    }

    public List<String> getFields() {
        return fields;
    }

    public String getModifiedAfter() {
        return modifiedAfter;
    }

    public String getModifiedBefore() {
        return modifiedBefore;
    }

    public String getFilterByFormula() {
        return filterByFormula;
    }

    public static class Builder {
        private List<String> fields = null;
        private String modifiedAfter = null;
        private String modifiedBefore = null;
        private String filterByFormula = null;

        public Builder fields(final List<String> fields) {
            this.fields = fields;
            return this;
        }

        public Builder field(final String field) {
            if (fields == null) {
                fields = new ArrayList<>();
            }
            fields.add(field);
            return this;
        }

        public Builder modifiedAfter(final String modifiedAfter) {
            this.modifiedAfter = modifiedAfter;
            return this;
        }

        public Builder modifiedBefore(final String modifiedBefore) {
            this.modifiedBefore = modifiedBefore;
            return this;
        }

        public Builder filterByFormula(final String filterByFormula) {
            this.filterByFormula = filterByFormula;
            return this;
        }

        public AirtableGetRecordsFilter build() {
            return new AirtableGetRecordsFilter(fields, modifiedAfter, modifiedBefore, filterByFormula);
        }
    }
}
