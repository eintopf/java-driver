/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.custom;
import static com.datastax.driver.core.DataType.list;

/**
 * Test we "support" custom types.
 */
public class CustomTypeTest extends CCMBridge.PerClassSingleNodeCluster {

    public static final DataType CUSTOM_DYNAMIC_COMPOSITE = custom(
        "org.apache.cassandra.db.marshal.DynamicCompositeType("
        + "s=>org.apache.cassandra.db.marshal.UTF8Type,"
        + "i=>org.apache.cassandra.db.marshal.Int32Type)");

    public static final DataType CUSTOM_COMPOSITE = custom(
        "org.apache.cassandra.db.marshal.CompositeType("
            + "org.apache.cassandra.db.marshal.UTF8Type,"
            + "org.apache.cassandra.db.marshal.Int32Type)");

    public static final DataType LIST_OF_INTS = list(cint());

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.singleton(
            "CREATE TABLE test ("
                + "    k int,"
                + "    c1 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',"
                + "    c2 'ReversedType(CompositeType(UTF8Type, Int32Type))'," // reversed translates to CLUSTERING ORDER BY DESC
                + "    c3 'ListType(Int32Type)'," // translates to list<int>
                + "    PRIMARY KEY (k, c1, c2)"
                + ")"
        );
    }

    @Test(groups = "short")
    public void should_serialize_and_deserialize_custom_types() {

        TableMetadata table = cluster.getMetadata().getKeyspace(keyspace).getTable("test");

        assertThat(table.getColumn("c1")).isClusteringColumn().hasType(CUSTOM_DYNAMIC_COMPOSITE);
        assertThat(table.getColumn("c2")).isClusteringColumn().hasType(CUSTOM_COMPOSITE).hasClusteringOrder(ClusteringOrder.DESC);
        assertThat(table.getColumn("c3")).hasType(LIST_OF_INTS);

        session.execute("INSERT INTO test(k, c1, c2, c3) VALUES (0, 's@foo:i@32', 'foo:32', [1,2,3])");
        session.execute("INSERT INTO test(k, c1, c2, c3) VALUES (0, 'i@42', ':42', [2,3,4])");
        session.execute("INSERT INTO test(k, c1, c2, c3) VALUES (0, 'i@12:i@3', 'foo', [3,4,5])");

        ResultSet rs = session.execute("SELECT * FROM test");

        Row r = rs.one();

        assertThat(r.getColumnDefinitions().getType("c1")).isEqualTo(CUSTOM_DYNAMIC_COMPOSITE);
        assertThat(r.getColumnDefinitions().getType("c2")).isEqualTo(CUSTOM_COMPOSITE);
        assertThat(r.getColumnDefinitions().getType("c3")).isEqualTo(LIST_OF_INTS);

        assertThat(r.getInt("k")).isEqualTo(0);
        assertThat(r.getBytesUnsafe("c1")).isEqualTo(serializeForDynamicCompositeType(12, 3));
        assertThat(r.getBytesUnsafe("c2")).isEqualTo(serializeForCompositeType("foo"));
        assertThat(r.getList("c3", Integer.class)).isEqualTo(newArrayList(3,4,5));

        r = rs.one();
        assertThat(r.getInt("k")).isEqualTo(0);
        assertThat(r.getBytesUnsafe("c1")).isEqualTo(serializeForDynamicCompositeType(42));
        assertThat(r.getBytesUnsafe("c2")).isEqualTo(serializeForCompositeType("", 42));
        assertThat(r.getList("c3", Integer.class)).isEqualTo(newArrayList(2,3,4));

        r = rs.one();
        assertThat(r.getInt("k")).isEqualTo(0);
        assertThat(r.getBytesUnsafe("c1")).isEqualTo(serializeForDynamicCompositeType("foo", 32));
        assertThat(r.getBytesUnsafe("c2")).isEqualTo(serializeForCompositeType("foo", 32));
        assertThat(r.getList("c3", Integer.class)).isEqualTo(newArrayList(1,2,3));
    }


    private ByteBuffer serializeForDynamicCompositeType(Object... params) {

        List<ByteBuffer> l = new ArrayList<ByteBuffer>();
        int size = 0;
        for (Object p : params) {
            if (p instanceof Integer) {
                ByteBuffer elt = ByteBuffer.allocate(2 + 2 + 4 + 1);
                elt.putShort((short)(0x8000 | 'i'));
                elt.putShort((short) 4);
                elt.putInt((Integer)p);
                elt.put((byte)0);
                elt.flip();
                size += elt.remaining();
                l.add(elt);
            } else if (p instanceof String) {
                ByteBuffer bytes = ByteBuffer.wrap(((String)p).getBytes());
                ByteBuffer elt = ByteBuffer.allocate(2 + 2 + bytes.remaining() + 1);
                elt.putShort((short)(0x8000 | 's'));
                elt.putShort((short) bytes.remaining());
                elt.put(bytes);
                elt.put((byte)0);
                elt.flip();
                size += elt.remaining();
                l.add(elt);
            } else {
                throw new RuntimeException();
            }
        }
        ByteBuffer res = ByteBuffer.allocate(size);
        for (ByteBuffer bb : l)
            res.put(bb);
        res.flip();
        return res;
    }

    private ByteBuffer serializeForCompositeType(Object... params) {

        List<ByteBuffer> l = new ArrayList<ByteBuffer>();
        int size = 0;
        for (Object p : params) {
            if (p instanceof Integer) {
                ByteBuffer elt = ByteBuffer.allocate(2 + 4 + 1);
                elt.putShort((short) 4);
                elt.putInt((Integer)p);
                elt.put((byte)0);
                elt.flip();
                size += elt.remaining();
                l.add(elt);
            } else if (p instanceof String) {
                ByteBuffer bytes = ByteBuffer.wrap(((String)p).getBytes());
                ByteBuffer elt = ByteBuffer.allocate(2 + bytes.remaining() + 1);
                elt.putShort((short) bytes.remaining());
                elt.put(bytes);
                elt.put((byte)0);
                elt.flip();
                size += elt.remaining();
                l.add(elt);
            } else {
                throw new RuntimeException();
            }
        }
        ByteBuffer res = ByteBuffer.allocate(size);
        for (ByteBuffer bb : l)
            res.put(bb);
        res.flip();
        return res;
    }

}
