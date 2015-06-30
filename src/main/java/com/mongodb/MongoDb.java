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
package com.mongodb;

import com.mongodb.OutMessage.OpCode;
import com.yourkit.probes.MethodPattern;
import com.yourkit.probes.OnEnterResult;
import com.yourkit.probes.Param;
import com.yourkit.probes.StringColumn;
import com.yourkit.probes.Table;
import com.yourkit.probes.ThrownException;

/**
 * Records basic information for reads & writes to MongoDB, requires some package-private classes from Mongo.
 *
 * @author Tom Wedge
 */
public class MongoDb {
    private static final MongoWriteOperationTable WRITE_OP_TABLE_INSTANCE = new MongoWriteOperationTable();

    private static final MongoReadOperationTable READ_OP_TABLE_INSTANCE = new MongoReadOperationTable();

    @MethodPattern({"com.mongodb.DBTCPConnector:say(com.mongodb.DB, com.mongodb.OutMessage, com.mongodb.WriteConcern, com.mongodb.DBPort)"})
    public static final class MongoWriteProbe {
        public static int onEnter(@Param(2) final OutMessage outMessage) {
            final int rowIndex = WRITE_OP_TABLE_INSTANCE.createRow();

            WRITE_OP_TABLE_INSTANCE.collection.setValue(rowIndex, outMessage.getNamespace());
            WRITE_OP_TABLE_INSTANCE.operation.setValue(rowIndex, outMessage.getOpCode().name());
            if (OpCode.OP_QUERY.equals(outMessage.getOpCode())) {
                WRITE_OP_TABLE_INSTANCE.query.setValue(rowIndex, outMessage.getQuery().toString());
            } else {
                WRITE_OP_TABLE_INSTANCE.query.setValue(rowIndex, "");
            }
            return rowIndex;
        }

        public static void onExit(@OnEnterResult final int runRow, @ThrownException final Throwable e) {
            WRITE_OP_TABLE_INSTANCE.closeRow(runRow, e);
        }
    }

    @MethodPattern({"com.mongodb.DBTCPConnector:call(com.mongodb.DB, com.mongodb.DBCollection, com.mongodb.OutMessage, com.mongodb.ServerAddress, "
        + "int, com.mongodb.ReadPreference, com.mongodb.DBDecoder)"})
    public static final class MongoReadProbe {
        public static int onEnter(@Param(3) final OutMessage outMessage) {
            final int rowIndex = READ_OP_TABLE_INSTANCE.createRow();

            READ_OP_TABLE_INSTANCE.collection.setValue(rowIndex, outMessage.getNamespace());
            READ_OP_TABLE_INSTANCE.operation.setValue(rowIndex, outMessage.getOpCode().name());
            if (OpCode.OP_QUERY.equals(outMessage.getOpCode())) {
                READ_OP_TABLE_INSTANCE.query.setValue(rowIndex, outMessage.getQuery().toString());
            } else {
                READ_OP_TABLE_INSTANCE.query.setValue(rowIndex, "");
            }
            return rowIndex;
        }

        public static void onExit(@OnEnterResult final int runRow, @ThrownException final Throwable e) {
            READ_OP_TABLE_INSTANCE.closeRow(runRow, e);
        }
    }

    private static class MongoReadOperationTable extends Table {
        private final StringColumn collection = new StringColumn("Collection");

        private final StringColumn operation = new StringColumn("Operation");

        private final StringColumn query = new StringColumn("Query");

        protected MongoReadOperationTable() {
            super(MongoDb.class, "Mongo Read Operation", Table.MASK_FOR_LASTING_EVENTS);
        }
    }

    private static class MongoWriteOperationTable extends Table {
        private final StringColumn collection = new StringColumn("Collection");

        private final StringColumn operation = new StringColumn("Operation");

        private final StringColumn query = new StringColumn("Query");

        protected MongoWriteOperationTable() {
            super(MongoDb.class, "Mongo Write Operation", Table.MASK_FOR_LASTING_EVENTS);
        }
    }
}
