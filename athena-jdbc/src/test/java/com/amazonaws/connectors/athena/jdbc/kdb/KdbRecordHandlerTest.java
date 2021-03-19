/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.connectors.athena.jdbc.kdb;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.connectors.athena.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.joda.time.LocalDateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import com.amazonaws.connectors.athena.jdbc.kdb.KdbQueryStringBuilder.DateCriteria;

public class KdbRecordHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbRecordHandlerTest.class);

    private KdbRecordHandler recordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private AmazonS3 amazonS3;
    private AWSSecretsManager secretsManager;
    private AmazonAthena athena;
    private TableName tableName;
    private Schema schema;

    @Before
    public void setup()
    {
        this.amazonS3 = Mockito.mock(AmazonS3.class);
        this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        this.athena = Mockito.mock(AmazonAthena.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(Mockito.mock(JdbcCredentialProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new KdbQueryStringBuilder("`");
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", JdbcConnectionFactory.DatabaseEngine.MYSQL,
                "mysql://jdbc:mysql://hostname/user=A&password=B");

        this.recordHandler = new KdbRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder);

        tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(KdbMetadataHandler.newField("testCol1", Types.MinorType.INT, KdbTypes.int_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("testCol2", Types.MinorType.VARCHAR, KdbTypes.symbol_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("testCol3", Types.MinorType.BIGINT, KdbTypes.long_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("r"       , Types.MinorType.FLOAT8, KdbTypes.real_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("testCol5", Types.MinorType.SMALLINT, KdbTypes.short_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("testCol6", Types.MinorType.TINYINT, KdbTypes.byte_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("testCol7", Types.MinorType.FLOAT8, KdbTypes.float_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("testCol8", Types.MinorType.BIT, KdbTypes.bit_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("date"     , Types.MinorType.DATEDAY, KdbTypes.date_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("z"        , Types.MinorType.VARCHAR, KdbTypes.timestamp_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("g"        , Types.MinorType.VARCHAR, KdbTypes.guid_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("str"      , Types.MinorType.VARCHAR, KdbTypes.list_of_char_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("c"        , Types.MinorType.VARCHAR, KdbTypes.char_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("t"        , Types.MinorType.VARCHAR, KdbTypes.time_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("ts"       , Types.MinorType.VARCHAR, KdbTypes.timespan_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("partition_name", Types.MinorType.VARCHAR, null));
        schema = schemaBuilder.build();
    }

    @Test
    public void makeExtractor() throws Exception {
        LOGGER.info("makeExtractor starting");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.VARCHAR.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.BIGINT.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.FLOAT4.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol5", Types.MinorType.SMALLINT.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol6", Types.MinorType.TINYINT.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol7", Types.MinorType.FLOAT8.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("testCol8", Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("d", Types.MinorType.DATEDAY.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("z", Types.MinorType.DATEMILLI.getType()).build());
        // schemaBuilder.addField(FieldBuilder.newBuilder("g"        , Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition_name", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();  
        schema.getFields();
        ResultSet rs = Mockito.mock(ResultSet.class); 
        Mockito.when(rs.getDate("f")).thenReturn(new Date(1970, 0, 1));
        DateDayExtractor extractor = (DateDayExtractor)this.recordHandler.makeExtractor(schema.getFields().get(0), rs, Maps.newHashMap()); 
        NullableDateDayHolder holder = new NullableDateDayHolder();
        extractor.extract(null, holder);
        Assert.assertEquals(1, holder.isSet);
        Assert.assertEquals(0, holder.value);
    }

    @Test
    public void newFloat8Extractor() throws Exception
    {
        LOGGER.info("newFloat8Extractor starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getFloat("r")).thenReturn(1.4f);
        NullableFloat8Holder dst = new NullableFloat8Holder();
        this.recordHandler.newFloat8Extractor(rs, "r", KdbMetadataHandler.newField("r", Types.MinorType.FLOAT8, KdbTypes.real_type)).extract(null, dst);
        Assert.assertEquals(1.4, dst.value, 0.000000001);
    }

    @Test
    public void newVarCharExtractor_char() throws Exception
    {
        LOGGER.info("newVarCharExtractor_char starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("c")).thenReturn('w');
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.recordHandler.newVarcharExtractor(rs, "c", KdbMetadataHandler.newField("c", Types.MinorType.VARCHAR, KdbTypes.char_type)).extract(null, dst);
        Assert.assertEquals("w", dst.value);
    }

    @Test
    public void newVarCharExtractor_str() throws Exception
    {
        LOGGER.info("newVarCharExtractor_str starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("str")).thenReturn("abc");
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.recordHandler.newVarcharExtractor(rs, "str", KdbMetadataHandler.newField("str", Types.MinorType.VARCHAR, KdbTypes.list_of_char_type)).extract(null, dst);
        Assert.assertEquals("abc", dst.value);
    }

    @Test
    public void newVarCharExtractor_timestamp1() throws Exception
    {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("str")).thenReturn(new Timestamp(2020 - 1900, 0, 2, 3, 4, 5, 0));
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.recordHandler.newVarcharExtractor(rs, "str", KdbMetadataHandler.newField("str", Types.MinorType.VARCHAR, KdbTypes.timestamp_type)).extract(null, dst);
        Assert.assertEquals("2020.01.02D03:04:05.000000000", dst.value);
    }

    @Test
    public void newVarCharExtractor_timestamp2() throws Exception
    {
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("str")).thenReturn(new Timestamp(2020 - 1900, 0, 2, 3, 4, 5, 1000000));
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.recordHandler.newVarcharExtractor(rs, "str", KdbMetadataHandler.newField("str", Types.MinorType.VARCHAR, KdbTypes.timestamp_type)).extract(null, dst);
        Assert.assertEquals("2020.01.02D03:04:05.001000000", dst.value);
    }


    @Test
    public void newVarCharExtractor_timespan() throws Exception
    {
        LOGGER.info("newVarCharExtractor_timespan starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("str")).thenReturn("03:04:05.6"); //actually this object type is Kx$Timespan
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.recordHandler.newVarcharExtractor(rs, "str", KdbMetadataHandler.newField("str", Types.MinorType.VARCHAR, KdbTypes.timespan_type)).extract(null, dst);
        Assert.assertEquals("03:04:05.600000000", dst.value);
    }

    @Test
    public void newVarCharExtractor_list_of_timestamp() throws Exception
    {
        LOGGER.info("newVarCharExtractor_list_of_timestamp starting...");
        Timestamp[] a = new Timestamp[] {
            new Timestamp(2020 - 1900, 0, 2, 3, 4, 5, 1000000),
            new Timestamp(Long.MIN_VALUE), //null value
            new Timestamp(2020 - 1900, 0, 2, 3, 4, 5, 1000000)
        };
        Assert.assertEquals("2020.01.02D03:04:05.001000000 0Np 2020.01.02D03:04:05.001000000", KdbRecordHandler.toVarChar(a));
    }

    @Test
    public void newVarCharExtractor_list_of_symbol() throws Exception
    {
        LOGGER.info("newVarCharExtractor_list_of_timestamp starting...");
        String[] a = new String[] { "abc", "", "def" };
        Assert.assertEquals("`abc``def", KdbRecordHandler.toVarChar(a));
    }

    @Test
    public void newVarCharExtractor_list_of_int() throws Exception
    {
        LOGGER.info("newVarCharExtractor_list_of_int starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("str")).thenReturn(new int[] { 1, Integer.MIN_VALUE, 2 });
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.recordHandler.newVarcharExtractor(rs, "str", KdbMetadataHandler.newField("str", Types.MinorType.VARCHAR, KdbTypes.list_of_int_type)).extract(null, dst);
        Assert.assertEquals("1 0Ni 2", dst.value);
    }

    @Test
    public void newVarCharExtractor_list_of_long() throws Exception
    {
        LOGGER.info("newVarCharExtractor_list_of_long starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("str")).thenReturn(new long[] { 1L, Long.MIN_VALUE, 2L });
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.recordHandler.newVarcharExtractor(rs, "str", KdbMetadataHandler.newField("str", Types.MinorType.VARCHAR, KdbTypes.list_of_long_type)).extract(null, dst);
        Assert.assertEquals("1 0Nj 2", dst.value);
    }

    @Test
    public void newVarCharExtractor_list_of_float() throws Exception
    {
        LOGGER.info("newVarCharExtractor_list_of_float starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("str")).thenReturn(new double[] { 1.0, 1.1 });
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.recordHandler.newVarcharExtractor(rs, "str", KdbMetadataHandler.newField("str", Types.MinorType.VARCHAR, KdbTypes.list_of_float_type)).extract(null, dst);
        Assert.assertEquals("1.0 1.1", dst.value);
    }

    @Test
    public void newVarCharExtractor_list_of_byte() throws Exception
    {
        LOGGER.info("newVarCharExtractor_list_of_byte starting...");
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.when(rs.getObject("str")).thenReturn(new byte[] { 0x00, 0x01 });
        NullableVarCharHolder dst = new NullableVarCharHolder();
        this.recordHandler.newVarcharExtractor(rs, "str", KdbMetadataHandler.newField("str", Types.MinorType.VARCHAR, KdbTypes.list_of_byte_type)).extract(null, dst);
        Assert.assertEquals("0x0001", dst.value);
    }

    @Test
    public void pushdown()
    {
        Assert.assertEquals("myfunc[1970.01.01;1970.01.01]", KdbQueryStringBuilder.pushDownDateCriteriaIntoFuncArgs("myfunc[2021.01.01;2021.01.01]", new DateCriteria(0, 0)));
        Assert.assertEquals("myfunc[1970.01.02;1970.01.03]", KdbQueryStringBuilder.pushDownDateCriteriaIntoFuncArgs("myfunc[2021.01.01;2021.01.01]", new DateCriteria(1, 2)));
    }

    @Test
    public void buildSplitSql_parallel_query()
            throws SQLException
    {
        LOGGER.info("buildSplitSql with parallel query starting");

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition_name", "2/2"));
        Mockito.when(split.getProperty(Mockito.eq("partition_name"))).thenReturn("2/2");

        Range range1a = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1a.isSingleValue()).thenReturn(true);
        Mockito.when(range1a.getLow().getValue()).thenReturn(1);
        Range range1b = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range1b.isSingleValue()).thenReturn(true);
        Mockito.when(range1b.getLow().getValue()).thenReturn(2);
        ValueSet valueSet1 = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet1.getRanges().getOrderedRanges()).thenReturn(ImmutableList.of(range1a, range1b));

        ValueSet valueSet2 = getSingleValueSet("abc");
        ValueSet valueSet3 = getRangeSet(Marker.Bound.ABOVE, 2L, Marker.Bound.EXACTLY, 20L);
        ValueSet valueSet4 = getSingleValueSet(1.5); //real
        ValueSet valueSet5 = getSingleValueSet(1);
        ValueSet valueSet6 = getSingleValueSet(0);
        ValueSet valueSet7 = getSingleValueSet(1.2d);
        ValueSet valueSet8 = getSingleValueSet(true);
        ValueSet valueSet9 = getRangeSet(Marker.Bound.EXACTLY, 0, Marker.Bound.EXACTLY, 2); getSingleValueSet(0); //1970-1-1 - 1970-1-3
        ValueSet valueSet10 = getSingleValueSet("2020.01.01D02:03:04.005006007"); //timespan new LocalDateTime(2020, 1, 1, 2, 3, 4, 5)
        ValueSet valueSet11 = getSingleValueSet("1234-5678");
        ValueSet valueSet_str = getSingleValueSet("xyz");
        ValueSet valueSet_c   = getSingleValueSet("w");
        ValueSet valueSet_t   = getSingleValueSet("03:03:04.005");
        ValueSet valueSet_ts  = getSingleValueSet("04:03:04.005006007");

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol1", valueSet1)
                .put("testCol2", valueSet2)
                .put("testCol3", valueSet3)
                .put("r"       , valueSet4)
                .put("testCol5", valueSet5)
                .put("testCol6", valueSet6)
                .put("testCol7", valueSet7)
                .put("testCol8", valueSet8)
                .put("date", valueSet9)
                .put("z"        , valueSet10)
                .put("g"        , valueSet11)
                .put("str"      , valueSet_str)
                .put("c"        , valueSet_c)
                .put("t"        , valueSet_t)
                .put("ts"       , valueSet_ts)
                .build());

        String expectedSql = "q) select testCol1, testCol2, testCol3, r, testCol5, testCol6, testCol7, testCol8, date, z, g, str, c, t, ts from testTable  where (date within (1970.01.02;1970.01.03)) , (testCol1 in (1i, 2i)) , (testCol2 = `abc) , ((testCol3 > 2) and (testCol3 <= 20)) , (r = 1.5e) , (testCol5 = 1i) , (testCol6 = 0i) , (testCol7 = 1.2) , (testCol8 = 1b) , ((date within (1970.01.01;1970.01.03))) , (z = 2020.01.01D02:03:04.005006007) , (g = \"G\"$\"1234-5678\") , (c = \"w\") , (t = 03:03:04.005) , (ts = 04:03:04.005006007)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.recordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_null()
            throws SQLException
    {
        LOGGER.info("buildSplitSql_null starting");

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition_name", "2021.01.02"));
        Mockito.when(split.getProperty(Mockito.eq("partition_name"))).thenReturn("2021.01.02");

        ValueSet valueSet1 = getSingleValueSetOnlyNull();
        ValueSet valueSet2 = getSingleValueSetOnlyNull();
        ValueSet valueSet3 = getSingleValueSetOnlyNull();
        ValueSet valueSet4 = getSingleValueSetOnlyNull();
        ValueSet valueSet5 = getSingleValueSetOnlyNull();
        ValueSet valueSet6 = getSingleValueSetOnlyNull();
        ValueSet valueSet7 = getSingleValueSetOnlyNull();
        ValueSet valueSet8 = getSingleValueSetOnlyNull();
        ValueSet valueSet9 = getSingleValueSetOnlyNull();
        ValueSet valueSet10 = getSingleValueSetOnlyNull();
        ValueSet valueSet11 = getSingleValueSetOnlyNull();
        ValueSet valueSet_str = getSingleValueSetOnlyNull();
        ValueSet valueSet_c = getSingleValueSetOnlyNull();
        ValueSet valueSet_t = getSingleValueSetOnlyNull();
        ValueSet valueSet_ts= getSingleValueSetOnlyNull();

        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol1", valueSet1)
                .put("testCol2", valueSet2)
                .put("testCol3", valueSet3)
                .put("r"       , valueSet4)
                .put("testCol5", valueSet5)
                .put("testCol6", valueSet6)
                .put("testCol7", valueSet7)
                .put("testCol8", valueSet8)
                .put("date", valueSet9)
                .put("z", valueSet10)
                .put("g"        , valueSet11)
                .put("str"      , valueSet_str)
                .put("c"        , valueSet_c)
                .put("t"        , valueSet_t)
                .put("ts"       , valueSet_ts)
                .build());

        String expectedSql = "q) select testCol1, testCol2, testCol3, r, testCol5, testCol6, testCol7, testCol8, date, z, g, str, c, t, ts from testTable  where (testCol1 = 0Ni) , (testCol2 = ` ) , (testCol3 = 0Nj) , (r = 0Ne) , (testCol5 = 0Nh) , (testCol6 = 0x00) , (testCol7 = 0n) , (testCol8 = 0b) , (date = 0Nd) , (z = 0Np) , (g = 0Ng) , (c = \" \") , (t = 0Nt) , (ts = 0Nn)";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);

        PreparedStatement preparedStatement = this.recordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    static private ValueSet getSingleValueSet(Object value) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    static private ValueSet getSingleValueSetOnlyNull() {
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.isNullAllowed()).thenReturn(true);
        Mockito.when(valueSet.isNone()).thenReturn(true); //collection is empty
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.emptyList());
        return valueSet;
    }

    static private ValueSet getRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue) {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getBound()).thenReturn(lowerBound);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getHigh().getBound()).thenReturn(upperBound);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }
}
