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

package org.apache.nifi.controller.status.history.storage.questdb;

import org.apache.nifi.controller.status.history.InMemoryComponentDetailsStorage;
import org.apache.nifi.controller.status.history.questdb.QuestDbContext;
import org.apache.nifi.controller.status.history.storage.StatusStorages;

public final class QuestDbStatusStoragesFactory {

    private QuestDbStatusStoragesFactory() {

    }

    public static StatusStorages create(final QuestDbContext dbContext) {
        return create(dbContext, new InMemoryComponentDetailsStorage());
    }

    public static StatusStorages create(final QuestDbContext dbContext, final InMemoryComponentDetailsStorage componentDetailsProvider) {
        return new StatusStorages(new QuestDbProcessorStatusStorage(dbContext, componentDetailsProvider),
                new QuestDbConnectionStatusStorage(dbContext, componentDetailsProvider),
                new QuestDbProcessGroupStatusStorage(dbContext, componentDetailsProvider),
                new QuestDbRemoteProcessGroupStatusStorage(dbContext, componentDetailsProvider),
                new QuestDbNodeStatusStorage(dbContext),
                new QuestDbGarbageCollectionStatusStorage(dbContext));
    }
}
