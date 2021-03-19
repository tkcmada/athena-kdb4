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

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.connectors.athena.jdbc.TestBase;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KdbMetadataHandlerTest
        extends TestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbMetadataHandlerTest.class);

    // private DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", JdbcConnectionFactory.DatabaseEngine.MYSQL,
    //         "mysql://jdbc:mysql://hostname/user=A&password=B");
    // private MySqlMetadataHandler mySqlMetadataHandler;
    // private JdbcConnectionFactory jdbcConnectionFactory;
    // private Connection connection;
    // private FederatedIdentity federatedIdentity;
    // private AWSSecretsManager secretsManager;
    // private AmazonAthena athena;

    @Before
    public void setup()
    {
        // this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        // this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        // Mockito.when(this.jdbcConnectionFactory.getConnection(Mockito.any(JdbcCredentialProvider.class))).thenReturn(this.connection);
        // this.secretsManager = Mockito.mock(AWSSecretsManager.class);
        // this.athena = Mockito.mock(AmazonAthena.class);
        // Mockito.when(this.secretsManager.getSecretValue(Mockito.eq(new GetSecretValueRequest().withSecretId("testSecret")))).thenReturn(new GetSecretValueResult().withSecretString("{\"username\": \"testUser\", \"password\": \"testPassword\"}"));
        // this.mySqlMetadataHandler = new MySqlMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, this.jdbcConnectionFactory);
        // this.federatedIdentity = Mockito.mock(FederatedIdentity.class);
    }

    @Test
    public void kdbTableNameToAthenaTableName_and_athenaTableNameToKdbTableNameTest() {
        Assert.assertEquals("rate"        , KdbMetadataHandler.kdbTableNameToAthenaTableName("Rate"));
        Assert.assertEquals("marketbooks" , KdbMetadataHandler.kdbTableNameToAthenaTableName("MarketBooks"));
        Assert.assertEquals("timezonetab" , KdbMetadataHandler.kdbTableNameToAthenaTableName("timezoneTab"));
        Assert.assertEquals("ohlc_30m"    , KdbMetadataHandler.kdbTableNameToAthenaTableName("OHLC_30m"));

        Assert.assertEquals("Rate"       , KdbMetadataHandler.athenaTableNameToKdbTableName("rate"));
        Assert.assertEquals("MarketBooks", KdbMetadataHandler.athenaTableNameToKdbTableName("marketbooks"));
        Assert.assertEquals("timezoneTab", KdbMetadataHandler.athenaTableNameToKdbTableName("timezonetab"));
        Assert.assertEquals("OHLC_30m"   , KdbMetadataHandler.athenaTableNameToKdbTableName("ohlc_30m"));

        //if mapping doesn't exist, just use naming rule.
        // Assert.assertEquals("`USDJPY;`V1" , KdbMetadataHandler.athenaTableNameToKdbTableName("`__usdjpy__;`_v1"));
        Assert.assertEquals("`USDJPY;`V1" , KdbMetadataHandler.athenaTableNameToKdbTableName("__`usdjpy;`v1__"));
        Assert.assertEquals("HOGE_30m", KdbMetadataHandler.athenaTableNameToKdbTableName("__hoge___30m"));
    }

    @Test
    public void addSchema() {
        LOGGER.info("addSchema start....");
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addIntField("x");
        schemaBuilder.addFloat8Field("f");
        schemaBuilder.addStringField("s");
        schemaBuilder.addField("d", new ArrowType.Date(DateUnit.DAY));
        // schemaBuilder.addField("t", new ArrowType.Time(TimeUnit.NANOSECOND, 128)); // Detected unsupported type[Time(NANOSECOND, 128) / TIMENANO for column[t]
        // schemaBuilder.addField("z", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")); // Detected unsupported type[Timestamp(NANOSECOND, UTC) / TIMESTAMPNANOTZ for column[z]
        Schema schema = schemaBuilder.build();

        for (Field next : schema.getFields()) {
             com.amazonaws.athena.connector.lambda.data.SupportedTypes.assertSupported(next);
        }
    }

    // @Test
    // public void getPartitionSchema()
    // {
    //     Assert.assertEquals(SchemaBuilder.newBuilder()
    //                     .addField(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build(),
    //             this.mySqlMetadataHandler.getPartitionSchema("testCatalogName"));
    // }

    // @Test
    // public void doGetTableLayout()
    //         throws Exception
    // {
    //     BlockAllocator blockAllocator = new BlockAllocatorImpl();
    //     Constraints constraints = Mockito.mock(Constraints.class);
    //     TableName tableName = new TableName("testSchema", "testTable");
    //     Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
    //     Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
    //     GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

    //     PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    //     Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

    //     String[] columns = {"partition_name"};
    //     int[] types = {Types.VARCHAR};
    //     Object[][] values = {{"p0"}, {"p1"}};
    //     ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
    //     Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

    //     Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

    //     GetTableLayoutResponse getTableLayoutResponse = this.mySqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

    //     Assert.assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

    //     List<String> expectedValues = new ArrayList<>();
    //     for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
    //         expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
    //     }
    //     Assert.assertEquals(expectedValues, Arrays.asList("[partition_name : p0]", "[partition_name : p1]"));

    //     SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
    //     expectedSchemaBuilder.addField(FieldBuilder.newBuilder(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
    //     Schema expectedSchema = expectedSchemaBuilder.build();
    //     Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
    //     Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

    //     Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getTableName());
    //     Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, tableName.getSchemaName());
    // }

    // @Test
    // public void doGetTableLayoutWithNoPartitions()
    //         throws Exception
    // {
    //     BlockAllocator blockAllocator = new BlockAllocatorImpl();
    //     Constraints constraints = Mockito.mock(Constraints.class);
    //     TableName tableName = new TableName("testSchema", "testTable");
    //     Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
    //     Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
    //     GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

    //     PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    //     Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

    //     String[] columns = {"partition_name"};
    //     int[] types = {Types.VARCHAR};
    //     Object[][] values = {{}};
    //     ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
    //     Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

    //     Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

    //     GetTableLayoutResponse getTableLayoutResponse = this.mySqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

    //     Assert.assertEquals(values.length, getTableLayoutResponse.getPartitions().getRowCount());

    //     List<String> expectedValues = new ArrayList<>();
    //     for (int i = 0; i < getTableLayoutResponse.getPartitions().getRowCount(); i++) {
    //         expectedValues.add(BlockUtils.rowToString(getTableLayoutResponse.getPartitions(), i));
    //     }
    //     Assert.assertEquals(expectedValues, Collections.singletonList("[partition_name : *]"));

    //     SchemaBuilder expectedSchemaBuilder = SchemaBuilder.newBuilder();
    //     expectedSchemaBuilder.addField(FieldBuilder.newBuilder(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()).build());
    //     Schema expectedSchema = expectedSchemaBuilder.build();
    //     Assert.assertEquals(expectedSchema, getTableLayoutResponse.getPartitions().getSchema());
    //     Assert.assertEquals(tableName, getTableLayoutResponse.getTableName());

    //     Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, tableName.getTableName());
    //     Mockito.verify(preparedStatement, Mockito.times(1)).setString(2, tableName.getSchemaName());
    // }

    // @Test(expected = RuntimeException.class)
    // public void doGetTableLayoutWithSQLException()
    //         throws Exception
    // {
    //     Constraints constraints = Mockito.mock(Constraints.class);
    //     TableName tableName = new TableName("testSchema", "testTable");
    //     Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
    //     Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
    //     GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

    //     Connection connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
    //     JdbcConnectionFactory jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
    //     Mockito.when(jdbcConnectionFactory.getConnection(Mockito.any(JdbcCredentialProvider.class))).thenReturn(connection);
    //     Mockito.when(connection.getMetaData().getSearchStringEscape()).thenThrow(new SQLException());
    //     MySqlMetadataHandler mySqlMetadataHandler = new MySqlMetadataHandler(databaseConnectionConfig, this.secretsManager, this.athena, jdbcConnectionFactory);

    //     mySqlMetadataHandler.doGetTableLayout(Mockito.mock(BlockAllocator.class), getTableLayoutRequest);
    // }

    // @Test
    // public void doGetSplits()
    //         throws Exception
    // {
    //     BlockAllocator blockAllocator = new BlockAllocatorImpl();
    //     Constraints constraints = Mockito.mock(Constraints.class);
    //     TableName tableName = new TableName("testSchema", "testTable");

    //     PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    //     Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

    //     String[] columns = {MySqlMetadataHandler.PARTITION_COLUMN_NAME};
    //     int[] types = {Types.VARCHAR};
    //     Object[][] values = {{"p0"}, {"p1"}};
    //     ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
    //     Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

    //     Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

    //     Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
    //     Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
    //     GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

    //     GetTableLayoutResponse getTableLayoutResponse = this.mySqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

    //     BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
    //     GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, null);
    //     GetSplitsResponse getSplitsResponse = this.mySqlMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

    //     Set<Map<String, String>> expectedSplits = new HashSet<>();
    //     expectedSplits.add(Collections.singletonMap(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, "p0"));
    //     expectedSplits.add(Collections.singletonMap(MySqlMetadataHandler.BLOCK_PARTITION_COLUMN_NAME, "p1"));
    //     Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
    //     Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
    //     Assert.assertEquals(expectedSplits, actualSplits);
    // }

    // @Test
    // public void doGetSplitsContinuation()
    //         throws Exception
    // {
    //     BlockAllocator blockAllocator = new BlockAllocatorImpl();
    //     Constraints constraints = Mockito.mock(Constraints.class);
    //     TableName tableName = new TableName("testSchema", "testTable");
    //     Schema partitionSchema = this.mySqlMetadataHandler.getPartitionSchema("testCatalogName");
    //     Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
    //     GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, constraints, partitionSchema, partitionCols);

    //     PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    //     Mockito.when(this.connection.prepareStatement(MySqlMetadataHandler.GET_PARTITIONS_QUERY)).thenReturn(preparedStatement);

    //     String[] columns = {"partition_name"};
    //     int[] types = {Types.VARCHAR};
    //     Object[][] values = {{"p0"}, {"p1"}};
    //     ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
    //     final String expectedQuery = String.format(MySqlMetadataHandler.GET_PARTITIONS_QUERY, tableName.getTableName(), tableName.getSchemaName());
    //     Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

    //     Mockito.when(this.connection.getMetaData().getSearchStringEscape()).thenReturn(null);

    //     GetTableLayoutResponse getTableLayoutResponse = this.mySqlMetadataHandler.doGetTableLayout(blockAllocator, getTableLayoutRequest);

    //     BlockAllocator splitBlockAllocator = new BlockAllocatorImpl();
    //     GetSplitsRequest getSplitsRequest = new GetSplitsRequest(this.federatedIdentity, "testQueryId", "testCatalogName", tableName, getTableLayoutResponse.getPartitions(), new ArrayList<>(partitionCols), constraints, "1");
    //     GetSplitsResponse getSplitsResponse = this.mySqlMetadataHandler.doGetSplits(splitBlockAllocator, getSplitsRequest);

    //     Set<Map<String, String>> expectedSplits = new HashSet<>();
    //     expectedSplits.add(Collections.singletonMap("partition_name", "p1"));
    //     Assert.assertEquals(expectedSplits.size(), getSplitsResponse.getSplits().size());
    //     Set<Map<String, String>> actualSplits = getSplitsResponse.getSplits().stream().map(Split::getProperties).collect(Collectors.toSet());
    //     Assert.assertEquals(expectedSplits, actualSplits);
    // }
}
