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

import java.beans.Transient;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcRecordHandler.SkipQueryException;
import com.google.common.collect.ImmutableMap;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker.Bound;
import com.amazonaws.connectors.athena.jdbc.kdb.KdbQueryStringBuilder.DateCriteria;

public class KdbQueryStringBuilderTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbQueryStringBuilderTest.class);

    @Test
    public void toLiteral() throws Exception {
        LOGGER.info("toLiteral starting");

        Assert.assertEquals("1.5e"               , KdbQueryStringBuilder.toLiteral(1.5, MinorType.FLOAT8, KdbTypes.real_type));
        Assert.assertEquals("1970.01.02"         , KdbQueryStringBuilder.toLiteral(1, MinorType.DATEDAY, null));
        Assert.assertEquals("1970.01.04D00:00:00.004000000", KdbQueryStringBuilder.toLiteral(new org.joda.time.LocalDateTime(1970, 1, 4, 0, 0, 0, 4), MinorType.DATEMILLI, null));
        Assert.assertEquals("\"G\"$\"1234-5678\"", KdbQueryStringBuilder.toLiteral("1234-5678", MinorType.VARCHAR, KdbTypes.guid_type));

        //time
        Assert.assertEquals("00:00:00.001",
            KdbQueryStringBuilder.toLiteral("00:00:00.001", MinorType.VARCHAR, KdbTypes.time_type));
        Assert.assertEquals("0Nt",
            KdbQueryStringBuilder.toLiteral(null          , MinorType.VARCHAR, KdbTypes.time_type));

        //timespan
        Assert.assertEquals("00:00:00.001002003",
            KdbQueryStringBuilder.toLiteral("00:00:00.001002003", MinorType.VARCHAR, KdbTypes.timespan_type));
        Assert.assertEquals("0Nn",
            KdbQueryStringBuilder.toLiteral(null                , MinorType.VARCHAR, KdbTypes.timespan_type));

        //timestamp as string
        Assert.assertEquals("1970.01.02D00:00:00.001002003",
            KdbQueryStringBuilder.toLiteral("1970.01.02D00:00:00.001002003", MinorType.VARCHAR, KdbTypes.timestamp_type));
        Assert.assertEquals("0Np",
            KdbQueryStringBuilder.toLiteral(null                           , MinorType.VARCHAR, KdbTypes.timestamp_type));
        //timestamp as Timestamp
        Assert.assertEquals("2020.01.02D03:04:05.001000000",
            KdbQueryStringBuilder.toLiteral(new Timestamp(2020 - 1900, 0, 2, 3, 4, 5, 1000000), MinorType.VARCHAR, KdbTypes.timestamp_type));
    }

    @Test
    public void getDaysOf()
    {
        Assert.assertEquals("2021-08-30T00:00:00.000", DateTimeFormat.forPattern("yyyyMMdd").parseLocalDateTime("20210830").toString());

        Assert.assertEquals(0, KdbQueryStringBuilder.getDaysOf(new LocalDateTime("1970-01-01T14:05:31.789")));
        Assert.assertEquals(1, KdbQueryStringBuilder.getDaysOf(new LocalDateTime("1970-01-02T00:00:00.000")));
        Assert.assertEquals(1, KdbQueryStringBuilder.getDaysOf(new LocalDateTime("1970-01-02T14:05:31.789")));
        Assert.assertEquals(18869, KdbQueryStringBuilder.getDaysOf(new LocalDateTime("2021-08-30T14:20:00.000")));
        // The following case is passed when it's executed at 2011-08-30T14:57:00.000+09:00
        // Assert.assertEquals(18869, KdbQueryStringBuilder.getDaysOfToday(new LocalDateTime(DateTimeZone.forID("Asia/Tokyo"))));
        
    }

    @Test
    public void pushdown()
    {
        Assert.assertEquals("myfunc[1970.01.01;1970.01.01]", KdbQueryStringBuilder.pushDownDateCriteriaIntoFuncArgs("myfunc[2021.01.01;2021.01.01]", new DateCriteria(0, 0)));
        Assert.assertEquals("myfunc[1970.01.02;1970.01.03]", KdbQueryStringBuilder.pushDownDateCriteriaIntoFuncArgs("myfunc[2021.01.01;2021.01.01]", new DateCriteria(1, 2)));
        Assert.assertEquals("myfunc[2021.08.30;2021.08.30]", KdbQueryStringBuilder.pushDownDateCriteriaIntoFuncArgs("myfunc[2021.01.01;2021.01.01]", new DateCriteria(18869, 18869)));
    }

    @Test
    public void getDateRangeParallelQuery()
    {
        Assert.assertEquals("[0, 0, 1]", Arrays.toString(KdbQueryStringBuilder.getDateRangeParallelQuery(1 , 3)));
        Assert.assertEquals("[0, 0, 2]", Arrays.toString(KdbQueryStringBuilder.getDateRangeParallelQuery(2 , 3)));
        Assert.assertEquals("[1, 1, 1]", Arrays.toString(KdbQueryStringBuilder.getDateRangeParallelQuery(3 , 3)));
        Assert.assertEquals("[1, 1, 2]", Arrays.toString(KdbQueryStringBuilder.getDateRangeParallelQuery(4 , 3)));
        Assert.assertEquals("[1, 1, 3]", Arrays.toString(KdbQueryStringBuilder.getDateRangeParallelQuery(5 , 3)));
        Assert.assertEquals("[2, 2, 2]", Arrays.toString(KdbQueryStringBuilder.getDateRangeParallelQuery(6 , 3)));
    }

    @Test
    public void getDateRangeParallelQuery2()
    {
        Assert.assertEquals("0(1970.01.01)-0(1970.01.01)", KdbQueryStringBuilder.getDateRangeParallelQuery(new DateCriteria(0, 3), 3, 0).toString());
        Assert.assertEquals("1(1970.01.02)-1(1970.01.02)", KdbQueryStringBuilder.getDateRangeParallelQuery(new DateCriteria(0, 3), 3, 1).toString());
        Assert.assertEquals("2(1970.01.03)-3(1970.01.04)", KdbQueryStringBuilder.getDateRangeParallelQuery(new DateCriteria(0, 3), 3, 2).toString());

        Assert.assertEquals("0(1970.01.01)-1(1970.01.02)", KdbQueryStringBuilder.getDateRangeParallelQuery(new DateCriteria(0, 5), 3, 0).toString());
        Assert.assertEquals("2(1970.01.03)-3(1970.01.04)", KdbQueryStringBuilder.getDateRangeParallelQuery(new DateCriteria(0, 5), 3, 1).toString());
        Assert.assertEquals("4(1970.01.05)-5(1970.01.06)", KdbQueryStringBuilder.getDateRangeParallelQuery(new DateCriteria(0, 5), 3, 2).toString());
        
        try
        {
            KdbQueryStringBuilder.getDateRangeParallelQuery(new DateCriteria(0, 0), 2, 0);
            Assert.fail();
        }
        catch(SkipQueryException expected) {}
        Assert.assertEquals("0(1970.01.01)-0(1970.01.01)", KdbQueryStringBuilder.getDateRangeParallelQuery(new DateCriteria(0, 0), 2, 1).toString());

    }

    @Test
    public void getDateRange()
    {
        LocalDateTime today = LocalDateTime.parse("1970-01-05T08:00:00.000");
        
        Assert.assertEquals(new DateCriteria(0, 0), KdbQueryStringBuilder.getDateRange(KdbRecordHandlerTest.getSingleValueSet(0) , today));
        Assert.assertEquals(new DateCriteria(1, 1), KdbQueryStringBuilder.getDateRange(KdbRecordHandlerTest.getSingleValueSet(1) , today));

        Assert.assertEquals(new DateCriteria(1, 2), KdbQueryStringBuilder.getDateRange(KdbRecordHandlerTest.getRangeSet(Marker.Bound.EXACTLY, 1, Marker.Bound.EXACTLY, 2) , today));

        Assert.assertEquals(new DateCriteria(1, 4), KdbQueryStringBuilder.getDateRange(KdbRecordHandlerTest.getRangeSetLowerOnly(Marker.Bound.EXACTLY, 1) , today));
        Assert.assertNull(                          KdbQueryStringBuilder.getDateRange(KdbRecordHandlerTest.getRangeSetLowerOnly(Marker.Bound.ABOVE  , 1) , today));
        Assert.assertNull(                          KdbQueryStringBuilder.getDateRange(KdbRecordHandlerTest.getRangeSetLowerOnly(Marker.Bound.BELOW  , 1) , today));
    }

    private Schema schema;
    private Split split;
    private TimeManager timemgr;
    private Constraints constraints;
    private KdbQueryStringBuilder builder;

    private void setup()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(KdbMetadataHandler.newField("time", Types.MinorType.VARCHAR, KdbTypes.timestamp_type));
        schemaBuilder.addField(KdbMetadataHandler.newField("date", Types.MinorType.DATEDAY, KdbTypes.date_type));
        schema = schemaBuilder.build();

        split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.<String,String>builder()
            .put(KdbMetadataHandler.PARTITION_COLUMN_NAME, "*")
            .build());

        timemgr = Mockito.mock(TimeManager.class);
        Mockito.when(timemgr.newLocalDateTime(DateTimeZone.forID("Asia/Tokyo"))).thenReturn(LocalDateTime.parse("1970-01-05T08:00:00.000"));

        constraints = Mockito.mock(Constraints.class);
        Map<String, ValueSet> summary = ImmutableMap.<String, ValueSet>builder()
            .put("date", KdbRecordHandlerTest.getRangeSet(Bound.EXACTLY, 1, Bound.EXACTLY, 2)) // date between 1970.01.02 and 1970.01.03
            .build();
        Mockito.when(constraints.getSummary()).thenReturn(summary);
    
        builder = new KdbQueryStringBuilder("`", timemgr);
    }

    @Test
    public void buildSql_datepushdown_between() throws SQLException
    {
        setup();

        String resultSql = builder.buildSqlString(
            "lambda:kdb"
            , "datepushdown=true"
            , "func_cfd[2021.01.01;2021.01.01]"
            , schema
            , constraints
            , split
            );
        
        Assert.assertEquals("q) select time, date from func_cfd[1970.01.02;1970.01.03]  where (date within (1970.01.02;1970.01.03)) , ((date within (1970.01.02;1970.01.03)))", resultSql);
    }

    @Test
    public void buildSql_datepushdown_singlevalue() throws SQLException
    {
        setup();
        Map<String, ValueSet> summary = ImmutableMap.<String, ValueSet>builder()
            .put("date", KdbRecordHandlerTest.getSingleValueSet(1)) // date == 1970.01.02
            .build();
        Mockito.when(constraints.getSummary()).thenReturn(summary);

        String resultSql = builder.buildSqlString(
            "lambda:kdb"
            , "datepushdown=true"
            , "func_cfd[2021.01.01;2021.01.01]"
            , schema
            , constraints
            , split
            );
        
        Assert.assertEquals("q) select time, date from func_cfd[1970.01.02;1970.01.02]  where (date within (1970.01.02;1970.01.02)) , (date = 1970.01.02)", resultSql);
    }

    @Test
    public void buildSql_datepushdown_only_lowerbound() throws SQLException
    {
        setup();
        Map<String, ValueSet> summary = ImmutableMap.<String, ValueSet>builder()
            .put("date", KdbRecordHandlerTest.getRangeSetLowerOnly(Bound.EXACTLY, 1)) // date >= 1970.01.02
            .build();
        Mockito.when(constraints.getSummary()).thenReturn(summary);

        String resultSql = builder.buildSqlString(
            "lambda:kdb"
            , "datepushdown=true"
            , "func_cfd[2021.01.01;2021.01.01]"
            , schema
            , constraints
            , split
            );
        
        Assert.assertEquals("q) select time, date from func_cfd[1970.01.02;1970.01.05]  where (date within (1970.01.02;1970.01.05)) , ((date >= 1970.01.02))", resultSql);
    }

    @Test
    public void buildSql_datepushdown_only_lowerbound_with_explicit_upperdate() throws SQLException
    {
        setup();

        Map<String, ValueSet> summary = ImmutableMap.<String, ValueSet>builder()
            .put("date", KdbRecordHandlerTest.getRangeSetLowerOnly(Bound.EXACTLY, 1)) // date >= 1970.01.02
            .build();
        Mockito.when(constraints.getSummary()).thenReturn(summary);

        String resultSql = builder.buildSqlString(
            "lambda:kdb"
            , "datepushdown=true&upperdate=19700103&lowerdateadjust=-1&upperdateadjust=1"
            , "func_cfd[2021.01.01;2021.01.01]"
            , schema
            , constraints
            , split
            );
        
        Assert.assertEquals("q) select time, date from func_cfd[1970.01.01;1970.01.04]  where (date within (1970.01.01;1970.01.04)) , ((date >= 1970.01.02))", resultSql);
    }

    @Test
    public void buildSql_datepushdown_with_invalid_upperdateadjust_format() throws SQLException
    {
        setup();

        Map<String, ValueSet> summary = ImmutableMap.<String, ValueSet>builder()
            .put("date", KdbRecordHandlerTest.getRangeSetLowerOnly(Bound.EXACTLY, 1)) // date >= 1970.01.02
            .build();
        Mockito.when(constraints.getSummary()).thenReturn(summary);

        try 
        {
            builder.buildSqlString(
                "lambda:kdb"
                , "datepushdown=true&upperdate=19700103&lowerdateadjust=-1&upperdateadjust=abc"
                , "func_cfd[2021.01.01;2021.01.01]"
                , schema
                , constraints
                , split
                );
            Assert.fail();
        }
        catch(IllegalArgumentException ex)
        {
            Assert.assertEquals("upperdateadjust should be integer format but was abc", ex.getMessage());
        }
    }

    public void buildSql_datepushdown_with_invalid_lowerdateadjust_format() throws SQLException
    {
        setup();

        Map<String, ValueSet> summary = ImmutableMap.<String, ValueSet>builder()
            .put("date", KdbRecordHandlerTest.getRangeSetLowerOnly(Bound.EXACTLY, 1)) // date >= 1970.01.02
            .build();
        Mockito.when(constraints.getSummary()).thenReturn(summary);

        try 
        {
            builder.buildSqlString(
                "lambda:kdb"
                , "datepushdown=true&upperdate=19700103&lowerdateadjust=xyz&upperdateadjust=1"
                , "func_cfd[2021.01.01;2021.01.01]"
                , schema
                , constraints
                , split
                );
            Assert.fail();
        }
        catch(IllegalArgumentException ex)
        {
            Assert.assertEquals("lowerdateadjust should be integer format but was xyz", ex.getMessage());
        }
    }

    @Test
    public void buildSql_datepushdown_only_lowerbound_timestamp() throws SQLException
    {
        setup();

        Map<String, ValueSet> summary = ImmutableMap.<String, ValueSet>builder()
            .put("time", KdbRecordHandlerTest.getRangeSetLowerOnly(Bound.EXACTLY, new org.apache.arrow.vector.util.Text("1970.01.02D09:00:00.000000000")))
            .build();
        Mockito.when(constraints.getSummary()).thenReturn(summary);

        String resultSql = builder.buildSqlString(
            "lambda:kdb"
            , "datepushdown=true&lowerdateadjust=-1&upperdateadjust=1"
            , "func_cfd[2021.01.01;2021.01.01]"
            , schema
            , constraints
            , split
            );
        
        Assert.assertEquals("q) select time, date from func_cfd[1970.01.01;1970.01.06]  where (date within (1970.01.01;1970.01.06)) , ((time >= 1970.01.02D09:00:00.000000000))", resultSql);
    }


    @Test
    public void buildSql_parallel_1_of_2() throws SQLException
    {
        setup();
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.<String,String>builder()
            .put(KdbMetadataHandler.PARTITION_COLUMN_NAME, "1/2")
            .build());

        String resultSql = builder.buildSqlString(
            "lambda:kdb"
            , "datepushdown=true&para=2"
            , "func_cfd[2021.01.01;2021.01.01]"
            , schema
            , constraints
            , split
            );
        
        Assert.assertEquals("q) select time, date from func_cfd[1970.01.02;1970.01.02]  where (date within (1970.01.02;1970.01.02)) , ((date within (1970.01.02;1970.01.03)))", resultSql);
    }

    @Test
    public void buildSql_parallel_2_of_2() throws SQLException
    {
        setup();
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.<String,String>builder()
            .put(KdbMetadataHandler.PARTITION_COLUMN_NAME, "2/2")
            .build());

        String resultSql = builder.buildSqlString(
            "lambda:kdb"
            , "datepushdown=true&para=2"
            , "func_cfd[2021.01.01;2021.01.01]"
            , schema
            , constraints
            , split
            );
        
        Assert.assertEquals("q) select time, date from func_cfd[1970.01.03;1970.01.03]  where (date within (1970.01.03;1970.01.03)) , ((date within (1970.01.02;1970.01.03)))", resultSql);
    }
}
