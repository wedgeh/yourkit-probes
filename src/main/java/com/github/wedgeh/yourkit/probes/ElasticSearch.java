/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.wedgeh.yourkit.probes;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;

import com.yourkit.probes.MethodPattern;
import com.yourkit.probes.ObjectRowIndexMap;
import com.yourkit.probes.OnEnterResult;
import com.yourkit.probes.ReturnValue;
import com.yourkit.probes.StringColumn;
import com.yourkit.probes.Table;
import com.yourkit.probes.This;
import com.yourkit.probes.ThrownException;

/**
 * Records the execution of elastic search operations.
 *
 * @author Tom Wedge
 */
public class ElasticSearch {
    private static final ElasticSearchOperationTable ES_OP_TABLE_INSTANCE = new ElasticSearchOperationTable();
    /**
     * Mapping between action response objects and row index in ES_OP_TABLE_INSTANCE table
     */
    private static final ObjectRowIndexMap<ListenableActionFuture<? extends ActionResponse>> RESPONSE_TO_ROW_INDEX = new ObjectRowIndexMap<ListenableActionFuture<? extends ActionResponse>>();

    @MethodPattern({"org.elasticsearch.action.ActionRequestBuilder:execute()"})
    public static final class ElasticSearchStartExecutionProbe {
        public static int onEnter(@This final ActionRequestBuilder actionRequestBuilder) {
            final int rowIndex = ES_OP_TABLE_INSTANCE.createRow();

            ES_OP_TABLE_INSTANCE.builderClass.setValue(rowIndex, actionRequestBuilder.getClass().getCanonicalName());
            String value = null;
            if (actionRequestBuilder instanceof IndexRequestBuilder) {
                value = ((IndexRequestBuilder) actionRequestBuilder).request().toString();
            } else if (actionRequestBuilder instanceof SearchRequestBuilder) {
                value = ((SearchRequestBuilder) actionRequestBuilder).toString();
            } else if (actionRequestBuilder instanceof SearchScrollRequestBuilder) {
                ES_OP_TABLE_INSTANCE.scrollId.setValue(rowIndex, ((SearchScrollRequestBuilder) actionRequestBuilder).request().scrollId());
            }
            if (value != null) {
                ES_OP_TABLE_INSTANCE.operationValue.setValue(rowIndex, value);
            }

            return rowIndex;
        }

        public static void onExit(@OnEnterResult final int runRow, @ThrownException final Throwable e,
            @ReturnValue final ListenableActionFuture<? extends ActionResponse> returnVal) {
            RESPONSE_TO_ROW_INDEX.put(returnVal, runRow);
        }
    }

    @MethodPattern({"org.elasticsearch.action.support.*:actionGet()"})
    public static final class ElasticSearchCompleteExecutionProbe {
        public static int onEnter(@This final ListenableActionFuture<? extends ActionResponse> actionResponse) {
            if (RESPONSE_TO_ROW_INDEX.containsKey(actionResponse)) {
                return RESPONSE_TO_ROW_INDEX.get(actionResponse);
            } else {
                return Table.NO_ROW;
            }
        }

        public static void onExit(@OnEnterResult final int runRow, @ThrownException final Throwable e, @ReturnValue final Object returnVal) {
            if (!(Table.NO_ROW == runRow)) {
                if (returnVal instanceof SearchResponse) {
                    ES_OP_TABLE_INSTANCE.scrollId.setValue(runRow, ((SearchResponse) returnVal).getScrollId());
                }
                ES_OP_TABLE_INSTANCE.closeRow(runRow, e);
            }
        }
    }

    private static class ElasticSearchOperationTable extends Table {
        private final StringColumn builderClass = new StringColumn("Operation Builder Class");

        private final StringColumn operationValue = new StringColumn("Operation Value");

        private final StringColumn scrollId = new StringColumn("Scroll ID");

        protected ElasticSearchOperationTable() {
            super(ElasticSearch.class, "Elastic Search Operation", Table.MASK_FOR_LASTING_EVENTS);
        }
    }
}
