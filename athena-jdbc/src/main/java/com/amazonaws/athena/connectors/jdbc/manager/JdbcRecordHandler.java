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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;

import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import com.amazonaws.athena.connectors.jdbc.connection.RdsSecretsCredentialProvider;
import com.amazonaws.athena.connectors.jdbc.kdb.KdbMetadataHandler;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Abstracts JDBC record handler and provides common reusable split records handling.
 */
public abstract class JdbcRecordHandler
        extends RecordHandler
{
    public static final org.joda.time.MutableDateTime EPOCH = new org.joda.time.MutableDateTime(1970, 1, 1, 0, 0, 0, 0); //1970-01-01 00:00:00.000
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordHandler.class);
    protected final JdbcConnectionFactory jdbcConnectionFactory;
    private final DatabaseConnectionConfig databaseConnectionConfig;

    /**
     * Used only by Multiplexing handler. All invocations will be delegated to respective database handler.
     */
    protected JdbcRecordHandler()
    {
        super(null);
        this.jdbcConnectionFactory = null;
        this.databaseConnectionConfig = null;
    }

    protected JdbcRecordHandler(final AmazonS3 amazonS3, final AWSSecretsManager secretsManager, AmazonAthena athena, final DatabaseConnectionConfig databaseConnectionConfig,
            final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig.getType().getDbName());
        this.jdbcConnectionFactory = Validate.notNull(jdbcConnectionFactory, "jdbcConnectionFactory must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
    }

    protected JdbcConnectionFactory getJdbcConnectionFactory()
    {
        return jdbcConnectionFactory;
    }

    protected JdbcCredentialProvider getCredentialProvider()
    {
        final String secretName = this.databaseConnectionConfig.getSecret();
        if (StringUtils.isNotBlank(secretName)) {
            return new RdsSecretsCredentialProvider(getSecret(secretName));
        }

        return null;
    }
    
    public static class SkipQueryException extends RuntimeException
    {
        public SkipQueryException(String message)
        {
            super(message);
        }
    }

    @Override
    public void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker)
    {
        LOGGER.info("readWithConstraint {}: Catalog: {}, table {}, splits {}", readRecordsRequest.getQueryId(), readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                readRecordsRequest.getSplit().getProperties());
        try (Connection connection = this.jdbcConnectionFactory.getConnection(getCredentialProvider())) {
            connection.setAutoCommit(false); // For consistency. This is needed to be false to enable streaming for some database types.
            final String parition_name = readRecordsRequest.getSplit().getProperties() == null ? "null split properties" : readRecordsRequest.getSplit().getProperties().get(KdbMetadataHandler.PARTITION_COLUMN_NAME);
            LOGGER.info("EXECUTE QUERY START:parition_name={}", parition_name);
            final long startMsec = System.currentTimeMillis();
            try (PreparedStatement preparedStatement = buildSplitSql(connection, readRecordsRequest.getCatalogName(), readRecordsRequest.getTableName(),
                    readRecordsRequest.getSchema(), readRecordsRequest.getConstraints(), readRecordsRequest.getSplit());
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                final long endMsec = System.currentTimeMillis();
                LOGGER.info("EXECUTE QUERY END:parition_name={}, elapsedSec={}", parition_name, (endMsec - startMsec) / 1000);
                Map<String, String> partitionValues = readRecordsRequest.getSplit().getProperties();

                GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(readRecordsRequest.getConstraints());
                for (Field next : readRecordsRequest.getSchema().getFields()) {
                    if (next.getType() instanceof ArrowType.List) {
                        rowWriterBuilder.withFieldWriterFactory(next.getName(), makeFactory(next));
                    }
                    else {
                        rowWriterBuilder.withExtractor(next.getName(), makeExtractor(next, resultSet, partitionValues));
                    }
                }

                GeneratedRowWriter rowWriter = rowWriterBuilder.build();
                int rowsReturnedFromDatabase = 0;
                while (resultSet.next()) {
                    if (!queryStatusChecker.isQueryRunning()) {
                        return;
                    }
                    blockSpiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, resultSet) ? 1 : 0);
                    rowsReturnedFromDatabase++;
                }
                LOGGER.info("{} rows returned by database.", rowsReturnedFromDatabase);

                connection.commit();
            }
            catch(SkipQueryException ex)
            {
                LOGGER.info("skipping query {}", ex.getMessage());
            }
        }
        catch (SQLException sqlException) {
            throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException.getMessage(), sqlException);
        }
    }

    /**
     * Create a field extractor for complex List type.
     * @param field Field's metadata information.
     * @return Extractor for the List type.
     */
    protected FieldWriterFactory makeFactory(Field field)
    {
        return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                (FieldWriter) (Object context, int rowNum) ->
                {
                    try{
                        Object objary = ((ResultSet) context).getObject(field.getName());
                        if(objary != null && (objary instanceof String[]))
                        {
                            List<Object> fieldValue = new ArrayList<>(Arrays.asList((Object[])objary));
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                        }
                        else if(objary != null && (objary instanceof double[]))
                        {

                            // List<Object> fieldValue = new ArrayList<>(Arrays.asList((double[])objary)); //this doesn't work
                            double[] a = (double[])objary;
                            List<Object> fieldValue = new ArrayList<>();
                            for(double v : a)
                                fieldValue.add(v);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                        }
                        else if(objary != null && (objary instanceof float[]))
                        {
                            // List<Object> fieldValue = new ArrayList<>(Arrays.asList((float[])objary));
                            float[] a = (float[])objary;
                            List<Object> fieldValue = new ArrayList<>();
                            for(float v : a)
                                fieldValue.add(v);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                        }
                        else if(objary != null && (objary instanceof short[]))
                        {
                            // List<Object> fieldValue = new ArrayList<>(Arrays.asList((short[])objary));
                            short[] a = (short[])objary;
                            List<Object> fieldValue = new ArrayList<>();
                            for(short v : a)
                                fieldValue.add(v);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                        }
                        else if(objary != null && (objary instanceof long[]))
                        {
                            // List<Object> fieldValue = new ArrayList<>(Arrays.asList((long[])objary));
                            final long[] a = (long[])objary;
                            List<Object> fieldValue = new ArrayList<>();
                            for(long v : a)
                                fieldValue.add(v);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                        }
                        else if(objary != null && (objary instanceof int[]))
                        {
                            // List<Object> fieldValue = new ArrayList<>(Arrays.asList((int[])objary));
                            final int[] a = (int[])objary;
                            List<Object> fieldValue = new ArrayList<>();
                            for(int v : a)
                                fieldValue.add(v);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                        }
                        else if(objary != null && (objary instanceof byte[]))
                        {
                            // List<Object> fieldValue = new ArrayList<>(Arrays.asList((byte[])objary));
                            final byte[] a = (byte[])objary;
                            List<Object> fieldValue = new ArrayList<>();
                            for(byte v : a)
                                fieldValue.add(v);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                        }
                        else if(objary != null && (objary instanceof boolean[]))
                        {
                            // List<Object> fieldValue = new ArrayList<>(Arrays.asList((boolean[])objary));
                            final boolean[] a = (boolean[])objary;
                            List<Object> fieldValue = new ArrayList<>();
                            for(boolean v : a)
                                fieldValue.add(v);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                        }
                        else if(objary != null && (objary instanceof char[]))
                        {
                            LOGGER.warn("field " + field.getName() + " got char[]");
                            // List<Object> fieldValue = new ArrayList<>(Arrays.asList((char[])objary));
                            final char[] a = (char[])objary;
                            List<Object> fieldValue = new ArrayList<>();
                            for(char v : a)
                                fieldValue.add(v);
                            BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                        }
                        else
                        {
                            throw new SQLException("unknown type " + field.getName() + " " + (objary == null ? "null" : objary.getClass().getName()));
                        }
                        // else
                        // {
                        //     Array arrayField = ((ResultSet) context).getArray(field.getName()); //even double[] field, this cause exception
                        //     if (!((ResultSet) context).wasNull()) {
                        //         List<Object> fieldValue = new ArrayList<>(Arrays.asList((Object[]) arrayField.getArray()));
                        //         BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, fieldValue);
                        //     }
                        // }
                        return true;
                    }
                    catch(Exception ex)
                    {
                        final ResultSet resultSet = ((ResultSet) context);
                        final String fieldName = field.getName();
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=list " + info + " error=" + ex.getMessage(), ex);
                    }
                };
    }

    /**
     * Creates an Extractor for the given field. In this example the extractor just creates some random data.
     */
    @VisibleForTesting
    public Extractor makeExtractor(Field field, ResultSet resultSet, Map<String, String> partitionValues)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());

        final String fieldName = field.getName();

        if (partitionValues.containsKey(fieldName)) {
            return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
            {
                dst.isSet = 1;
                dst.value = partitionValues.get(fieldName);
            };
        }

        switch (fieldType) {
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) ->
                {
                    try
                    {
                        boolean value = resultSet.getBoolean(fieldName);
                        dst.value = value ? 1 : 0;
                        dst.isSet = resultSet.wasNull() ? 0 : 1;
                    }
                    catch(Exception ex)
                    {
                        final Object objval = resultSet.getObject(fieldName);
                        if(objval != null && objval.getClass().isArray())
                        {
                            Object[] ary = (Object[])objval;

                        }
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=" + fieldType + " " + info + " error=" + ex.getMessage(), ex);
                    }
                };
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) ->
                {
                    try
                    {
                        dst.value = resultSet.getByte(fieldName);
                        dst.isSet = resultSet.wasNull() ? 0 : 1;
                    }
                    catch(Exception ex)
                    {
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=" + fieldType + " " + info + " error=" + ex.getMessage(), ex);
                    }
                };
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) ->
                {
                    try
                    {
                        dst.value = resultSet.getShort(fieldName);
                        dst.isSet = resultSet.wasNull() ? 0 : 1;
                    }
                    catch(Exception ex)
                    {
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=" + fieldType + " " + info + " error=" + ex.getMessage(), ex);
                    }
                };
            case INT:
                return (IntExtractor) (Object context, NullableIntHolder dst) ->
                {
                    try
                    {
                        dst.value = resultSet.getInt(fieldName);
                        dst.isSet = resultSet.wasNull() ? 0 : 1;
                    }
                    catch(Exception ex)
                    {
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=" + fieldType + " " + info + " error=" + ex.getMessage(), ex);
                    }
                };
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) ->
                {
                    try
                    {
                        dst.value = resultSet.getLong(fieldName);
                        dst.isSet = resultSet.wasNull() ? 0 : 1;
                    }
                    catch(Exception ex)
                    {
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=" + fieldType + " " + info + " error=" + ex.getMessage(), ex);
                    }
                };
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) ->
                {
                    try
                    {
                        dst.value = resultSet.getFloat(fieldName);
                        dst.isSet = resultSet.wasNull() ? 0 : 1;
                    }
                    catch(Exception ex)
                    {
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=" + fieldType + " " + info + " error=" + ex.getMessage(), ex);
                    }
                };
            case FLOAT8:
                return newFloat8Extractor(resultSet, fieldName, field);
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) ->
                {
                    try
                    {
                        dst.value = resultSet.getBigDecimal(fieldName);
                        dst.isSet = resultSet.wasNull() ? 0 : 1;
                    }
                    catch(Exception ex)
                    {
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=" + fieldType + " " + info + " error=" + ex.getMessage(), ex);
                    }
                };
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) ->
                {
                    try
                    {
                        if (resultSet.getDate(fieldName) != null) {
                            // dst.value = (int) TimeUnit.MILLISECONDS.toDays(resultSet.getDate(fieldName).getTime());
                            java.sql.Date date = resultSet.getDate(fieldName);
                            org.joda.time.DateTime date2 = new org.joda.time.DateTime( ((java.util.Date) date).getTime() );
                            dst.value = org.joda.time.Days.daysBetween(EPOCH, date2).getDays();
                            if(LOGGER.isDebugEnabled()) LOGGER.debug("date field value:" + date + " " + date.getClass().getName() + " EPOCH = " + EPOCH + " date2 = " + date2 + " dst.value=" + dst.value);
                        }
                        dst.isSet = resultSet.wasNull() ? 0 : 1;
                    }
                    catch(Exception ex)
                    {
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=" + fieldType + " " + info + " error=" + ex.getMessage(), ex);
                    }
                };
            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    try
                    {
                        if (resultSet.getTimestamp(fieldName) != null) {
                            dst.value = resultSet.getTimestamp(fieldName).getTime();
                        }
                        dst.isSet = resultSet.wasNull() ? 0 : 1;
                    }
                    catch(Exception ex)
                    {
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=" + fieldType + " " + info + " error=" + ex.getMessage(), ex);
                    }
                };
            case VARCHAR:
                return newVarcharExtractor(resultSet, fieldName, field);
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) ->
                {
                    try
                    {
                        dst.value = resultSet.getBytes(fieldName);
                        dst.isSet = resultSet.wasNull() ? 0 : 1;
                    }
                    catch(Exception ex)
                    {
                        String info = getResultSetValueInfo(resultSet, fieldName);
                        throw new SQLException("Error occured. type=" + fieldType + " " + info + " error=" + ex.getMessage(), ex);
                    }
                };
            case LIST:
                return null; //this indicates that makeFactory will be called.
            case STRUCT:
                return null; //this indicates that makeFactory will be called.
            default:
                throw new RuntimeException("Unhandled type " + fieldType);
        }
    }

    static public String getResultSetValueInfo(ResultSet resultSet, String fieldName) throws SQLException
    {
        StringBuilder s = new StringBuilder();
        final Object objval = resultSet.getObject(fieldName);
        s.append("field=");
        s.append(fieldName);
        s.append(" objtype=");
        s.append(objval == null ? "null" : objval.getClass().getName());
        if(objval instanceof Object[])
        {
            s.append(" value(ary)=");
            Object[] ary = (Object[])objval;
            for(int i = 0; i < ary.length; i++)
            {
                if(i > 0)
                    s.append(",");
                Object v = ary[i];
                s.append(i);
                s.append(":");
                s.append(String.valueOf(v));
                s.append(":");
                s.append(v == null ? "null" : v.getClass().getName());
                if(i > 3)
                {
                    s.append("...");
                    break;
                }
            }
            s.append(" len=");
            s.append(ary.length);
            
            /*
            Array sary = resultSet.getArray(fieldName); //getArray throws SQLFeatureNotSupportedException
            ary = (Object[])sary.getArray();
            s.append(" sqlarylen=" + ary.length);
            for(int i = 0; i < ary.length; i++)
            {
                if(i > 0)
                    s.append(",");
                Object v = ary[i];
                s.append(i);
                s.append(":");
                s.append(String.valueOf(v));
                s.append(":");
                s.append(v == null ? "null" : v.getClass().getName());
                if(i > 3)
                {
                    s.append("...");
                    break;
                }
            }
            */
        }
        else
        {
            s.append(" value(obj)=");
            s.append(String.valueOf(objval));
        }
        return s.toString();
    } 

    /**
     * Since GeneratedRowWriter doesn't yet support complex types (STRUCT, LIST) we use this to
     * create our own FieldWriters via customer FieldWriterFactory. In this case we are producing
     * FieldWriters that only work for our exact example schema. This will be enhanced with a more
     * generic solution in a future release.
     */
    protected FieldWriterFactory makeFactory(Field field, ResultSet resultSet, Map<String, String> partitionValues) {
        final Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        final String fieldName = field.getName();
        throw new RuntimeException("Unhandled type " + String.valueOf(fieldType) + " field name " + String.valueOf(fieldName));
    }

    protected Float8Extractor newFloat8Extractor(final ResultSet resultSet, final String fieldName, final Field field)
    {
        return (Float8Extractor) (Object context, NullableFloat8Holder dst) ->
        {
            try
            {
                dst.value = resultSet.getDouble(fieldName);
                dst.isSet = resultSet.wasNull() ? 0 : 1;
            }
            catch(Exception ex)
            {
                String info = getResultSetValueInfo(resultSet, fieldName);
                throw new SQLException("Error occured. type=float8 " + info + " error=" + ex.getMessage(), ex);
            }
        };

    }

    protected VarCharExtractor newVarcharExtractor(final ResultSet resultSet, final String fieldName, final Field field)
    {
        return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
        {
            try
            {
                Object value = resultSet.getString(fieldName);
                if(value != null) {
                    dst.value = value.toString();
                }
                dst.isSet = resultSet.wasNull() ? 0 : 1;
            }
            catch(Exception ex)
            {
                String info = getResultSetValueInfo(resultSet, fieldName);
                throw new SQLException("Error occured. type=varchar " + info + " error=" + ex.getMessage(), ex);
            }
};
    }

    protected VarCharExtractor newListExtractor(final ResultSet resultSet, final String fieldName, final Field field)
    {
        return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
        {
            Object value = resultSet.getString(fieldName);
            if(value != null) {
                dst.value = value.toString();
            }
            dst.isSet = resultSet.wasNull() ? 0 : 1;
        };
    }

    /**
     * Builds split SQL string and returns prepared statement.
     *
     * @param jdbcConnection jdbc connection. See {@link Connection}
     * @param catalogName Athena provided catalog name.
     * @param tableName database table name.
     * @param schema table schema.
     * @param constraints constraints to push down to the database.
     * @param split table split.
     * @return prepared statement with sql. See {@link PreparedStatement}
     * @throws SQLException JDBC database exception.
     */
    public abstract PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
            throws SQLException;
}
