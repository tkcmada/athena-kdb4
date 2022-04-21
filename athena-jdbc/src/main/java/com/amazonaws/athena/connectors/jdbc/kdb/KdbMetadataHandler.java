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
package com.amazonaws.athena.connectors.jdbc.kdb;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles metadata for MySQL. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class KdbMetadataHandler
        extends JdbcMetadataHandler
{
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String GET_PARTITIONS_QUERY = "SELECT DISTINCT partition_name FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ? " +
            "AND partition_name IS NOT NULL";
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition_name";
    static final String ALL_PARTITIONS = "*";
    public static final String PARTITION_COLUMN_NAME = "partition_name";
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(KdbMetadataHandler.class);
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    public static final String KDBTYPE_KEY = "kdbtype";
    public static final String KDBTYPECHAR_KEY = "kdbtypechar";
    public static final String DEFAULT_SCHEMA_NAME = "schema1";

    public static final String SCHEMA_PARALLEL_KEY = "para";
    public static final String SCHEMA_DATEPUSHDOWN_KEY = "datepushdown";
    public static final String SCHEMA_TIMESTAMPFIELD_KEY = "timestampfield";
    public static final String SCHEMA_UPPERDATE_KEY = "upperdate";
    public static final String SCHEMA_LOWERDATEADJUST_KEY = "lowerdateadjust";
    public static final String SCHEMA_UPPERDATEADJUST_KEY = "upperdateadjust";
    public static final String SCHEMA_NOWHEREONDATEPUSHDOWN_KEY = "nowhereondatepushdown";
    public static final String SCHEMA_WHEREPUSHDOWN_KEY = "wherepushdown";
    public static final String SCHEMA_DATEFIELD_KEY = "datefield";
    public static final String IGNORE_UNKNOWN_TYPE_KEY = "ignore_unknown_type";
    public static final String REDIS_HOST_KEY = "REDIS_HOST";
    public static final String REDIS_PORT_KEY = "REDIS_PORT";
    public static final String FORCE_UPDATE_CACHE_KEY = "REDIS_CACHE_FORCE_UPDATE";
    public static final String REDIS_TABLE_SCHEME_CACHE_PREFIX = "KdbJdbcAthenaConnector:v1:tables:";

    private static boolean isListMappedToArray = true;

    public static boolean isListMappedToArray() { return isListMappedToArray; }
    public static void setListMappedToArray(boolean value) { isListMappedToArray = value; }

    private Jedis jedis;
    private boolean force_update_cache = false;
    
    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcCompositeHandler} instead.
     */
    public KdbMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(JdbcConnectionFactory.DatabaseEngine.KDB));
    }

    /**
     * Used by Mux.
     */
    public KdbMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        super(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES));
        createRedisClient();
    }

    @VisibleForTesting
    protected KdbMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
            AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
        createRedisClient();
    }

    private void createRedisClient()
    {
        String redishost = Objects.toString(System.getenv(REDIS_HOST_KEY), "").trim();
        String redisport = Objects.toString(System.getenv(REDIS_PORT_KEY), "").trim();
        LOGGER.info("redis=", redishost + ":" + redisport);
        if(redishost.isEmpty() || redisport.isEmpty())
        {
            LOGGER.info("no redis is specified.");
            return;
        }
        try{
            jedis = new Jedis(redishost, Integer.parseInt(redisport));
        }
        catch(NumberFormatException ex)
        {
            throw new IllegalArgumentException("Failed to parse Redis port into integer. Invalid port num=" + redisport);
        }
        force_update_cache = "true".equals(System.getenv(FORCE_UPDATE_CACHE_KEY));
    }

    @Override
    protected Set<String> listDatabaseNames(final Connection jdbcConnection)
            throws SQLException
    {
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        schemaNames.add(DEFAULT_SCHEMA_NAME); //kdb+ doesn't have schemas so returns default database.
        return schemaNames.build();
    }

    public static void cacheSchema(final Connection jdbcConnection) throws SQLException {
        if(! kdbtbl_by_athenatbl.isEmpty())
            return;
        LOGGER.info("schema is not cached yet. caching...");
        listTables(jdbcConnection);
    }

    @Override
    protected List<TableName> listTables(final Connection jdbcConnection, final String not_used_databaseName)
            throws SQLException
    {
        return listTables(jdbcConnection);
    }

    private static List<TableName> listTables(final Connection jdbcConnection)
            throws SQLException
    {
        LOGGER.info("listTables...");
        try (Statement stmt = jdbcConnection.createStatement()) {
            final String SCHEMA_QUERY = "q) flip ( `TABLE_NAME`TABLE_SCHEM ! ( tables[]; (count(tables[]))#(enlist \"" + DEFAULT_SCHEMA_NAME + "\") ) )";
            try (ResultSet resultSet = stmt.executeQuery(SCHEMA_QUERY)) {
                ImmutableList.Builder<TableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    LOGGER.info(String.format("list table:%s %s", resultSet.getObject("TABLE_SCHEM"), resultSet.getObject("TABLE_NAME")));
                    list.add(_getSchemaTableName(resultSet));
                }
                return list.build();
            }
        }
    }

    @Override
    protected TableName getSchemaTableName(final ResultSet resultSet)
            throws SQLException
    {
        return _getSchemaTableName(resultSet);
    }

    private static TableName _getSchemaTableName(final ResultSet resultSet)
            throws SQLException
    {
        return new TableName(
                resultSet.getString("TABLE_SCHEM"),
                kdbTableNameToAthenaTableName(resultSet.getString("TABLE_NAME")));
    }

    private static final Map<String, String> kdbtbl_by_athenatbl = new HashMap<>();

    @Override
    protected Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws SQLException
    {
        //Plese note that only following are supported in Athena as of 2020.05
        // BIT(Types.MinorType.BIT),
        // DATEMILLI(Types.MinorType.DATEMILLI),
        // DATEDAY(Types.MinorType.DATEDAY),
        // FLOAT8(Types.MinorType.FLOAT8),
        // FLOAT4(Types.MinorType.FLOAT4),
        // INT(Types.MinorType.INT),
        // TINYINT(Types.MinorType.TINYINT),
        // SMALLINT(Types.MinorType.SMALLINT),
        // BIGINT(Types.MinorType.BIGINT),
        // VARBINARY(Types.MinorType.VARBINARY),
        // DECIMAL(Types.MinorType.DECIMAL),
        // VARCHAR(Types.MinorType.VARCHAR),
        // STRUCT(Types.MinorType.STRUCT),
        // LIST(Types.MinorType.LIST);

        LOGGER.info("getSchema...");
        cacheSchema(jdbcConnection);

        final String athenaTableName = tableName.getTableName();
        LOGGER.info("Athena table name:" + athenaTableName);
        final String kdbTableName = athenaTableNameToKdbTableName(athenaTableName);
        LOGGER.info("Kdb table name:" + kdbTableName);
        SchemaBuilder schemaBuilder = getSchema(jdbcConnection, kdbTableName);

        // add partition columns
        partitionSchema.getFields().forEach(schemaBuilder::addField);

        Schema s = schemaBuilder.build();
        for (Field f : s.getFields()) {
            Types.MinorType mtype = Types.getMinorTypeForArrowType(f.getType());
            LOGGER.info(String.format("%s %s %s", f.getName(), f.getType(), mtype));
        }
        return s;        
    }

    Map<String, Character> getColumnAndType(final Connection jdbcConnection, final String kdbTableName) throws SQLException
    {
        LOGGER.info("getColumnAndType..." + kdbTableName);
        cacheTableSchema();
        for(String key : tableSchemaCache.keySet())
        {
            if(kdbTableName.contains(key))
            {
                LOGGER.info("table schema cache hit. " + key);
                return tableSchemaCache.get(key);
            }
        }
        if(jedis != null)
        {
            LOGGER.info("redis is on.");
            if(force_update_cache)
            {
                LOGGER.info("force update cache is on, so won't get table schema from redis.");
            }
            else
            {
                String col_and_types = jedis.get(REDIS_TABLE_SCHEME_CACHE_PREFIX + kdbTableName);
                if(col_and_types != null)
                {
                    LOGGER.info("got from cache. " + col_and_types);
                    final Map<String, Character> coltypes = createSingleTableSchemaFromString(col_and_types); 
                    LOGGER.info("got from cache. parsed. " + coltypes);
                    return coltypes;
                }
                LOGGER.info("no cache. " + kdbTableName);
            }
        }
        LOGGER.info("no table schema cache hit.");
        LinkedHashMap<String, Character> coltype = new LinkedHashMap<String, Character>();
        try (Statement stmt = jdbcConnection.createStatement()) {
            final String sql = "q) { flip `COLUMN_NAME`COLUMN_TYPE!(cols x; (value meta x)[;`t] ) }[" + kdbTableName + "]";
            //this may also work
            //q) { ([] COLUMN_NAME: cols x; COLUMN_TYPE: (value meta x)[`t]) }[ <kdbTableName here> ]
            LOGGER.info("Generated SQL for meta:" + sql);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String colname = rs.getString("COLUMN_NAME");
                    Character coltypeobj = (Character) rs.getObject("COLUMN_TYPE");
                    LOGGER.info("schema column mapping..." + colname + " " + String.valueOf(coltypeobj));
                    if(kdbTableName.contains(".athena.get_mosaic_data_sw") && colname.equals("done"))
                    {
                        coltypeobj = 'C';
                        LOGGER.info("override. schema column mapping..." + colname + " " + String.valueOf(coltypeobj));
                    }
                    if(coltypeobj == null) {
                        LOGGER.info("kdb+ type is unknown for column '" + colname + "' so assuming this col type is list of char(C)");
                        coltypeobj = 'C'; // fyi. 'C' is list of char , 'V' is list of list of char
                    }
                    coltype.put(colname, coltypeobj);
                }
            }
        }
        if(jedis != null)
        {
            String serstr = serializeTableSchema(coltype);
            LOGGER.info("setting on redis. " + REDIS_TABLE_SCHEME_CACHE_PREFIX + kdbTableName + "=" + serstr);
            jedis.set(REDIS_TABLE_SCHEME_CACHE_PREFIX + kdbTableName, serstr);
        }
        return coltype;
    }

    private static final HashMap<String, Map<String, Character>> tableSchemaCache = new HashMap<>();

    static void cacheTableSchema()
    {
        String str = Objects.toString(System.getenv("table_cache_string"), "").trim();
        LOGGER.info("TABLE_SCHEMA_CACHE env var = " + str);
        if(str.isEmpty())
        {
            LOGGER.info("no table schema cache given");
            return;
        }
        tableSchemaCache.putAll(createTableSchemaFromString(str));
    }

    static Map<String, Map<String, Character>> createTableSchemaFromString(String str)
    {
        Map<String, Map<String, Character>> map = new LinkedHashMap<>();
        for(String tbl_col_type : str.split("/"))
        {
            String[] tbl_col_type_ary = tbl_col_type.split("=", 2);
            String tblname = tbl_col_type_ary[0];
            String col_and_types = tbl_col_type_ary[1];
            final Map<String, Character> coltypes = createSingleTableSchemaFromString(col_and_types);
            map.put(tblname, coltypes);
            LOGGER.info("adding table schema cache. " + tblname + "=" + coltypes);
        }
        LOGGER.info("result table cache=" + map);
        return map;
    }

    static Map<String, Character> createSingleTableSchemaFromString(final String col_and_types)
    {
        LinkedHashMap<String, Character> coltypes = new LinkedHashMap<>();
        for(String col_type : col_and_types.split(","))
        {
            String[] col_type_ary = col_type.split(":", 2);
            String colname = col_type_ary[0];
            char   coltype = col_type_ary[1].charAt(0);
            coltypes.put(colname, coltype);
        }
        return coltypes;
    }

    static String serializeTableSchema(final Map<String, Character> coltypes)
    {
        StringBuilder b = new StringBuilder();
        for(Entry<String, Character> e : coltypes.entrySet())
        {
            if(b.length() > 0)
                b.append(",");
            b.append(e.getKey());
            b.append(":");
            b.append(e.getValue());
        }
        return b.toString();
    }

    SchemaBuilder getSchema(final Connection jdbcConnection, final String kdbTableName) throws SQLException
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        Map<String, Character> coltypes = getColumnAndType(jdbcConnection, kdbTableName);
        for(Entry<String, Character> e : coltypes.entrySet())
        {
            String colname = e.getKey();
            char coltype = (char)e.getValue();
            switch (coltype) {
                case 'b':
                    schemaBuilder.addField(newField(colname, Types.MinorType.BIT, KdbTypes.bit_type));
                    break;
                case 'x':
                    schemaBuilder.addField(newField(colname, Types.MinorType.TINYINT, KdbTypes.byte_type));
                    break;
                case 'h':
                    schemaBuilder.addField(newField(colname, Types.MinorType.SMALLINT, KdbTypes.short_type));
                    break;
                case 'i':
                    schemaBuilder.addField(newField(colname, Types.MinorType.INT, KdbTypes.int_type));
                    break;
                case 'j':
                    schemaBuilder.addField(newField(colname, Types.MinorType.BIGINT, KdbTypes.long_type));
                    break;
                case 'e': //real is mapped to Float8 but actual kdb type is real
                    schemaBuilder.addField(newField(colname, Types.MinorType.FLOAT8, KdbTypes.real_type));
                    break;
                case 'f':
                    schemaBuilder.addField(newField(colname, Types.MinorType.FLOAT8, KdbTypes.float_type));
                    break;
                case 'c': //char is mapped to VARCHAR because Athena doesn't have 
                    schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.char_type));
                    break;
                case 's': //symbol
                    schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.symbol_type));
                    break;
                case 'C': //list of char
                    schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_char_type));
                    break;
                case 'g': //guid
                    schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.guid_type));
                    break;
                case 'p': //timestamp
                    //Athena doesn't support DATENANO so map to VARCHAR for now
                    schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.timestamp_type));
                    break;
                case 't': //time //Athena doesn't support TIMEMILL
                    //Jdbc automatically map this column to java.sql.Time which has only sec precision
                //     schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.time_type));
                    //just map to VARCHAR for now
                    schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.timespan_type));
                    break;
                case 'n': //timespan //Athena doesn't support TIMENANO
                    //just map to VARCHAR for now
                    schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.timespan_type));
                    break;
                case 'd':
                    schemaBuilder.addField(newField(colname, Types.MinorType.DATEDAY, KdbTypes.date_type));
                    break;
                case 'J':
                    if (isListMappedToArray())
                    {
                        schemaBuilder.addField(newListField(colname, KdbTypes.list_of_long_type, Types.MinorType.BIGINT, KdbTypes.long_type));
                    }
                    else
                    {
                        schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_long_type));
                    }
                    break;
                case 'I':
                    if (isListMappedToArray())
                    {
                        schemaBuilder.addField(newListField(colname, KdbTypes.list_of_int_type, Types.MinorType.INT, KdbTypes.int_type));
                    }
                    else
                    {
                        schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_int_type));
                    }
                    break;
                case 'X':
                    if (isListMappedToArray())
                    {
                        schemaBuilder.addField(newListField(colname, KdbTypes.list_of_byte_type, Types.MinorType.TINYINT, KdbTypes.byte_type));
                    }
                    else
                    {
                        schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_byte_type));
                    }
                    break;
                case 'F':
                    if (isListMappedToArray())
                    {
                        schemaBuilder.addField(newListField(colname, KdbTypes.list_of_float_type, Types.MinorType.FLOAT8, KdbTypes.float_type));
                    }
                    else
                    {
                        schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_float_type));
                    }
                    break;
                case 'S':
                    if (isListMappedToArray())
                    {
                        schemaBuilder.addField(newListField(colname, KdbTypes.list_of_symbol_type, Types.MinorType.VARCHAR, KdbTypes.symbol_type));
                    }
                    else
                    {
                        schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_symbol_type));
                    }
                    break;
                case 'P':
                    if (isListMappedToArray())
                    {
                        schemaBuilder.addField(newListField(colname, KdbTypes.list_of_timestamp_type, Types.MinorType.VARCHAR, KdbTypes.timestamp_type));
                    }
                    else
                    {
                        schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_timestamp_type));
                    }
                    break;
                case 'V': //list of list of char
                    if (isListMappedToArray())
                    {
                        schemaBuilder.addField(newListField(colname, KdbTypes.list_of_list_of_char_type, Types.MinorType.VARCHAR, KdbTypes.list_of_char_type));
                    }
                    else
                    {
                        schemaBuilder.addField(newField(colname, Types.MinorType.VARCHAR, KdbTypes.list_of_list_of_char_type));
                    }
                    break;
                default:
                    String ignore = Objects.toString(System.getenv(IGNORE_UNKNOWN_TYPE_KEY), "").trim();
                    if(ignore.equals("true"))
                    {
                        LOGGER.warn("getSchema: Unable to map type for column[" + colname + "] to a supported type, attempted '" + coltype + "'");
                    }
                    else
                    {
                        throw new IllegalArgumentException("Unknown kdb column type " + coltype + " for column " + colname);
                    }
            }
        }
// q)(2i;2.3;`qwe;2000.01.02;12:34:56.000;2000.01.02D12:34:56.000000000)
// (2i;2.3;`qwe;2000.01.02;12:34:56.000;2000.01.02D12:34:56.000000000)

// q)t
// x f   s   d          t            z                            
// ---------------------------------------------------------------
// 2 2.3 qwe 2000.01.02 12:34:56.000 2000.01.02D12:34:56.000000000
// q)metat
// 'metat
//   [0]  metat
//        ^
// q)meta t
// c| t f a
// -| -----
// x| i    
// f| f    
// s| s    
// d| d    
// t| t    
// z| p    

        // try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData())) {
        //     boolean found = false;
        //     while (resultSet.next()) {
        //         ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
        //                 resultSet.getInt("DATA_TYPE"),
        //                 resultSet.getInt("COLUMN_SIZE"),
        //                 resultSet.getInt("DECIMAL_DIGITS"));
        //         String columnName = resultSet.getString("COLUMN_NAME");
        //         if (columnType != null && SupportedTypes.isSupported(columnType)) {
        //             schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
        //             found = true;
        //         }
        //         else {
        //             LOGGER.error("getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
        //         }
        //     }

        //     if (!found) {
        //         throw new RuntimeException("Could not find table in " + tableName.getSchemaName());
        //     }

            return schemaBuilder;
        // }
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        LOGGER.info("getPartitionSchema {}", catalogName);
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(newField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR, KdbTypes.list_of_char_type));
        return schemaBuilder.build();
    }
    
    private static final Map<String, String> EMPTY_MAP = ImmutableMap.<String, String>builder().build();
    public static Map<String, String> getProperties(final String schema)
    {
        if(! schema.contains("="))
            return EMPTY_MAP;
        Map<String, String> props = new HashMap<>();
        for(String keyval : schema.split("&"))
        {
            final String[] a = keyval.split("=", 2);
            final String key = a[0];
            final String val = a.length > 1 ? a[1] : "";
            props.put(key, val);
        }
        return props;
    }
    
    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker)
    {
        final String schemaname = getTableLayoutRequest.getTableName().getSchemaName();
        LOGGER.info("getPartitions {}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), schemaname,
                getTableLayoutRequest.getTableName().getTableName());
        String para = String.valueOf(getProperties(schemaname).get(SCHEMA_PARALLEL_KEY));
        LOGGER.info("para={}", para);
        int num_parallel_query = 1;
        try { num_parallel_query = Integer.parseInt(para); } catch(NumberFormatException ignored) {}
        LOGGER.info("num_parallel_query={}",num_parallel_query);
        if(num_parallel_query <= 1) { //single partition
            blockWriter.writeRows((Block block, int rowNum) -> {
                block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                LOGGER.info("Single parition. Adding all partition {}", ALL_PARTITIONS);
                //we wrote 1 row so we return 1
                return 1;
            });
        }
        else
        {
            for(int i = 1; i <= num_parallel_query; i++) {
                String partitionName = i + "/" + num_parallel_query;
                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
                    LOGGER.info("Adding partition {}", partitionName);
                    //we wrote 1 row so we return 1
                    return 1;
                });
            }
        }
    }

    @Override
    public GetSplitsResponse doGetSplits(
            final BlockAllocator blockAllocator, final GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("doGetSplits {}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();

        // TODO consider splitting further depending on #rows or data size. Could use Hash key for splitting if no partitions.
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            FieldReader locationReader = partitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
            locationReader.setPosition(curPartition);

            SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);

            LOGGER.info("{}: Input partition is {}", getSplitsRequest.getQueryId(), locationReader.readText());

            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                    .add(BLOCK_PARTITION_COLUMN_NAME, String.valueOf(locationReader.readText()));

            splits.add(splitBuilder.build());

            if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, encodeContinuationToken(curPartition));
            }
        }

        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken());
        }

        //No continuation token present
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }

    @VisibleForTesting
    static Field newField(String colname, Types.MinorType minorType, KdbTypes kdbtype)
    {
        final Map<String, String> metadata = ImmutableMap.<String, String>builder()
            .put(KDBTYPE_KEY    , kdbtype == null ? "null" : kdbtype.name())
            .put(KDBTYPECHAR_KEY, kdbtype == null ? " " : String.valueOf(kdbtype.kdbtype))
            .build();
        FieldType fieldType = new FieldType(true, minorType.getType(), null, metadata);
        return new Field(colname, fieldType, null);
    }

    @VisibleForTesting
    static Field newListField(String colname, KdbTypes listkdbtype, Types.MinorType primitive_minorType, KdbTypes primitive_kdbtype)
    {
        final Map<String, String> metadata = ImmutableMap.<String, String>builder()
            .put(KDBTYPE_KEY    , listkdbtype == null ? "null" : listkdbtype.name())
            .put(KDBTYPECHAR_KEY, listkdbtype == null ? " " : String.valueOf(listkdbtype.kdbtype))
            .build();
            
        FieldType fieldtype = new FieldType(false, new ArrowType.List(), null, metadata);

        Field baseField = newField("", primitive_minorType, primitive_kdbtype);
        Field listfield = new Field(colname,
                fieldtype,
                Collections.singletonList(baseField));
        return listfield;
    }

    @VisibleForTesting
    static char getKdbTypeChar(Field field)
    {
        return field.getMetadata().get(KDBTYPECHAR_KEY).charAt(0);
    }

    private static final ThreadLocal<Pattern> athenaTableNamePattern2 = new ThreadLocal<Pattern>() {
        @Override
        public Pattern initialValue()
        {
            return Pattern.compile("__([^_]+?)__");
        }
    };

    // private static final ThreadLocal<Pattern> athenaTableNamePattern = new ThreadLocal<Pattern>() {
    //     @Override
    //     public Pattern initialValue()
    //     {
    //         return Pattern.compile("_([a-z])");
    //     }
    // };

    private static final ThreadLocal<Pattern> kdbTableNamePattern = new ThreadLocal<Pattern>() {
        @Override
        public Pattern initialValue()
        {
            return Pattern.compile("[A-Z]");
        }
    };

    /**
     * convert Athena table name to Kdb table name.<p>
     * 
     * i.e. __usdjpy__ is converted into USDJPY
     * i.e. _rate is converted into Rate.<br>
     * i.e. _market_books is converted into MarketBooks.<br>
     */
    static public String athenaTableNameToKdbTableName(String athenaTableName)
    {
        String kdbTableName = kdbtbl_by_athenatbl.get(athenaTableName);
        if(kdbTableName != null)
            return kdbTableName;
        if(athenaTableName.contains("["))
        {
            try
            {
                final List<String> kdbnames = getKdbFunctionList();
                for(String kdbname : kdbnames)
                {
                    String athenaname = kdbname.toLowerCase();
                    if(athenaname.length() <= athenaTableName.length() && athenaTableName.substring(0, athenaname.length()).equals(athenaname))
                    {
                        if(athenaname.length() == athenaTableName.length())
                            athenaTableName = kdbname;
                        else
                            athenaTableName = kdbname + athenaTableName.substring(athenaname.length());
                        break;
                    }
                }
            }
            catch(IOException ex)
            {
                LOGGER.warn("cannot get getKdbFunctionList", ex);
            }
        }
        //if table mapping doesn't exist, just apply naming rule.
        String s = to_upper_case(athenaTableName, athenaTableNamePattern2.get());
        //return to_upper_case(s, athenaTableNamePattern.get());
        return s;
    }

    static private String to_upper_case(String src, Pattern pattern)
    {
        final StringBuilder dst = new StringBuilder();
        final Matcher m = pattern.matcher(src);
        int p = 0;
        while (m.find(p)) {
            dst.append(src.substring(p, m.start()));
            dst.append(m.group(1).toUpperCase());
            p = m.end();
        }
        dst.append(src.substring(p));
        return dst.toString();
    }

    /**
     * convert Athena table name to Kdb table name.<p>
     * 
     * i.e. _rate is converted into Rate.<br>
     * i.e. _market_books is converted into MarketBooks.<br>
     */
    static public String kdbTableNameToAthenaTableName(String kdbTableName)
    {
        String athenaTableName = kdbTableName.toLowerCase();
        LOGGER.info("register kdbTableNameToAthenaTableName mapping:" + athenaTableName + "->" + kdbTableName);
        kdbtbl_by_athenatbl.put(athenaTableName, kdbTableName);
        return athenaTableName;
        // final StringBuilder athenaTableName = new StringBuilder();
        // final Matcher m = kdbTableNamePattern.get().matcher(kdbTableName);
        // int p = 0;
        // while (m.find(p)) {
        //     athenaTableName.append(kdbTableName.substring(p, m.start()));
        //     athenaTableName.append("_");
        //     athenaTableName.append(m.group().toLowerCase());
        //     p = m.end();
        // }
        // athenaTableName.append(kdbTableName.substring(p));
        // return athenaTableName.toString();
    }
    
    private static FunctionResolver resolver = null;
    
    @VisibleForTesting
    static void setFunctionResolver(FunctionResolver resolver_)
    {
        resolver = resolver_;
    }
    
    public static void declareFunctionsFromS3(Connection conn) throws IOException, SQLException
    {
        LOGGER.info("declareFunctionsFromS3...");
        String s3region = Objects.toString(System.getenv("AWS_REGION"), "").trim();
        String s3bucket = Objects.toString(System.getenv("funcfile_s3bucket"), "").trim();
        String s3keys   = Objects.toString(System.getenv("funcfile_s3keys"), "").trim();
        LOGGER.info("funcfile region={}, bucket={}, keys={}", s3region, s3bucket, s3keys);
        if(s3region.isEmpty() || s3bucket.isEmpty() || s3keys.isEmpty())
        {
            LOGGER.info("no funcfile_s3region/bucket/keys are set.");
            return;
        }
        List<String> lines = S3Utils.getLinesFromS3(s3region, s3bucket, s3keys.split(","));
        try(Statement stmt = conn.createStatement()) {
            for(String line : lines) {
                LOGGER.info("executing..." + line);
                stmt.execute(line);
            }
        }
        LOGGER.info("declareFunctionsFromS3...done");
    }

    private static final List<String> EMPTY_LIST = ImmutableList.<String>builder().build();
    private static final List<String> funcListCache = new ArrayList<>();
    
    public static List<String> getKdbFunctionList() throws IOException
    {
        if(! funcListCache.isEmpty())
        {
            LOGGER.info("function list is already cached.");
            return funcListCache;
        }
        String s3region = Objects.toString(System.getenv("AWS_REGION"), "");
        String s3bucket = Objects.toString(System.getenv("funcmap_s3bucket"), "");
        String s3keys   = Objects.toString(System.getenv("funcmap_s3keys"), "");
        LOGGER.info("funclist region={}, bucket={}, keys={}", s3region, s3bucket, s3keys);
        if(resolver == null)
        {
            if(s3region.isEmpty() || s3bucket.isEmpty() || s3keys.isEmpty())
            {
                LOGGER.info("no funcmap_s3region/bucket/keys are set.");
                return EMPTY_LIST;
            }
            resolver = new S3FunctionResolver(s3region, s3bucket, s3keys);
        }
        LOGGER.info("use resolver to solve kdb function list.");
        funcListCache.addAll(resolver.getKdbFunctionList());
        return funcListCache;
    }
}
