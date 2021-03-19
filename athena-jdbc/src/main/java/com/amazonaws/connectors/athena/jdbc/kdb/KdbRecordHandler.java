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

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.connectors.athena.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.connectors.athena.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.manager.JDBCUtil;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ArrowBuf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;

/**
 * Data handler, user must have necessary permissions to read from necessary tables.
 */
public class KdbRecordHandler
        extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbRecordHandler.class);

    private static final String MYSQL_QUOTE_CHARACTER = "`";

    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    private static final int STRING_BUFFER_INITIAL_CAPACITY = 5000;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcCompositeHandler} instead.
     */
    public KdbRecordHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(JdbcConnectionFactory.DatabaseEngine.KDB));
    }

    public KdbRecordHandler(DatabaseConnectionConfig databaseConnectionConfig)
    {
        this(databaseConnectionConfig, AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient(),
                new GenericJdbcConnectionFactory(databaseConnectionConfig, KdbMetadataHandler.JDBC_PROPERTIES), new KdbQueryStringBuilder(MYSQL_QUOTE_CHARACTER));
    }

    @VisibleForTesting
    KdbRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, final AmazonS3 amazonS3, final AWSSecretsManager secretsManager,
            final AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory, final JdbcSplitQueryBuilder jdbcSplitQueryBuilder)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
        LOGGER.info("jdbcSplitQueryBuilder:" + jdbcSplitQueryBuilder.getClass().getName());
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
            throws SQLException
    {
        LOGGER.info("constraints:" + (String.valueOf(constraints)));
        LOGGER.info("split:" + (String.valueOf(split)));
        PreparedStatement preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(), schema, constraints, split);
LOGGER.info("pstmt:" + String.valueOf(preparedStatement));
        // Disable fetching all rows.
        preparedStatement.setFetchSize(Integer.MIN_VALUE);

        return preparedStatement;
    }

    @Override
    protected Float8Extractor newFloat8Extractor(final ResultSet resultSet, final String fieldName, final Field field)
    {
        return (Float8Extractor) (Object context, org.apache.arrow.vector.holders.NullableFloat8Holder dst) ->
        {
            final String kdbtype = field.getFieldType().getMetadata().get(KdbMetadataHandler.KDBTYPE_KEY);
            if (KdbTypes.real_type.name().equals(kdbtype)) {
                final float f = resultSet.getFloat(fieldName);
                dst.value = Double.parseDouble("" + f); //do not just cast from float to double as it would contain fraction
                dst.isSet = resultSet.wasNull() ? 0 : 1;
                LOGGER.info("Float8Extractor(float) " + String.valueOf(fieldName) + " " + dst.value + " float value=" + f);
            }
            else {
                dst.value = resultSet.getDouble(fieldName);
                dst.isSet = resultSet.wasNull() ? 0 : 1;
                LOGGER.info("Float8Extractor(double) " + String.valueOf(fieldName) + " " + dst.value + " double value=" + resultSet.getDouble(fieldName));
            }
        };
    }

    @Override
    protected VarCharExtractor newVarcharExtractor(final ResultSet resultSet, final String fieldName, final Field field)
    {
        return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
        {
            Object value = resultSet.getObject(fieldName);
            if (value != null) {
                final char kdbtypechar = KdbMetadataHandler.getKdbTypeChar(field);
                switch(kdbtypechar) {
                    case 'n': //timespan
                        final String timespanstr = value.toString() + "000000000";
                        dst.value = timespanstr.substring(0, 18);
                        break;
                    case 'p': //timestamp
                        final Timestamp timestamp = (Timestamp)value;
                        dst.value = KdbQueryStringBuilder.toLiteral(timestamp, Types.MinorType.VARCHAR, KdbTypes.timestamp_type);
                        break;
                    case 'S': //list of symbol
                        final String[] symbols = (String[]) value;
                        dst.value = toVarChar(symbols);
                        break;
                    case 'P': //list of timestamp
                        final Timestamp[] timestamps = (Timestamp[]) value;
                        dst.value = toVarChar(timestamps);
                        break;
                    case 'X': //list of byte
                        final byte[] bytes = (byte[]) value;
                        dst.value = toVarChar(bytes);
                        break;
                    case 'I': //list of int
                        final int[] ints = (int[]) value;
                        dst.value = toVarChar(ints);
                        break;
                    case 'J': //list of long
                        final long[] longs = (long[]) value;
                        dst.value = toVarChar(longs);
                        break;
                    case 'F': //list of float
                        final double[] doubles = (double[]) value;
                        dst.value = toVarChar(doubles);
                        break;
                    default:
                        dst.value = value.toString();
                }
            }
            dst.isSet = resultSet.wasNull() ? 0 : 1;
        };
    }

    @Override
    protected FieldWriterFactory makeFactory(Field field, ResultSet resultSet, Map<String, String> partitionValues)
    {
        final String fieldName = field.getName();
        final Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        final char kdbtypechar = KdbMetadataHandler.getKdbTypeChar(field);

        if (fieldType == MinorType.LIST)
        {
            final FieldWriterFactory factory = (FieldVector vector, Extractor extractor, ConstraintProjector constraint) -> {
                final FieldWriter fieldwriter = (Object context, int rowNum) -> {
                        final Object value = resultSet.getObject(fieldName);
                        if (value != null) {
                            UnionListWriter writer = ((ListVector) vector).getWriter();
                            writer.setPosition(rowNum);
                            writer.startList();
                            switch(kdbtypechar) {
                                case 'F': //list of float
                                    double[] doubles = (double[]) value;
                                    LOGGER.info(String.format("list at %s with constraints %s : %s ", rowNum, constraint, Arrays.toString(doubles)));
                                    for(int i = 0; i < doubles.length; i++) {
                                        //TODO should check constraints
                                        if(Double.isNaN(doubles[i])) {
                                            // writer.writeNull();
                                        }
                                        else {
                                            writer.writeFloat8(doubles[i]);
                                        }
                                    }
                                    break;
                                case 'J': //list of long
                                    long[] longs = (long[]) value;
                                    LOGGER.info(String.format("list at %s with constraints %s : %s ", rowNum, constraint, Arrays.toString(longs)));
                                    for(int i = 0; i < longs.length; i++) {
                                        //TODO should check constraints
                                        if(longs[i] == Long.MIN_VALUE) {
                                            // writer.writeNull();
                                        }
                                        else {
                                            writer.writeBigInt(longs[i]);
                                        }
                                    }
                                    break;
                                case 'I': //list of int
                                    int[] ints = (int[]) value;
                                    LOGGER.info(String.format("list at %s with constraints %s : %s ", rowNum, constraint, Arrays.toString(ints)));
                                    for(int i = 0; i < ints.length; i++) {
                                        //TODO should check constraints
                                        if(ints[i] == Integer.MIN_VALUE) {
                                            // writer.writeNull();
                                        }
                                        else {
                                            writer.writeInt(ints[i]);
                                        }
                                    }
                                    break;
                                case 'X': //list of byte
                                    byte[] bytes = (byte[]) value;
                                    LOGGER.info(String.format("list at %s with constraints %s : %s ", rowNum, constraint, Arrays.toString(bytes)));
                                    for(int i = 0; i < bytes.length; i++) {
                                        //TODO should check constraints
                                        writer.writeTinyInt(bytes[i]);
                                    }
                                    break;
                                case 'S': //list of symbol
                                    String[] symbols = (String[]) value;
                                    LOGGER.info(String.format("list at %s with constraints %s : %s ", rowNum, constraint, Arrays.toString(symbols)));
                                    for(int i = 0; i < symbols.length; i++) {
                                        //TODO should check constraints
                                        if (symbols[i] == null) {
                                            // writer.writeNull();
                                        }
                                        else {
                                            writeString(symbols[i], (ListVector) vector, writer);
                                        }
                                    }
                                    break;
                                case 'P': //list of timestamp
                                    Timestamp[] timestamps = (Timestamp[]) value;
                                    LOGGER.info(String.format("list at %s with constraints %s : %s ", rowNum, constraint, Arrays.toString(timestamps)));
                                    for(int i = 0; i < timestamps.length; i++) {
                                        //TODO should check constraints
                                        if (timestamps[i].getTime() == Long.MIN_VALUE) {
                                            // writer.writeNull();
                                        }
                                        else {
                                            writeString(KdbQueryStringBuilder.toLiteral(timestamps[i], Types.MinorType.VARCHAR, KdbTypes.timestamp_type), (ListVector) vector, writer);
                                        }
                                    }
                                    break;
                                case 'V': //list of string(list of list of char)
                                    Object[] strings = (Object[]) value;
                                    LOGGER.info(String.format("list at %s with constraints %s : %s ", rowNum, constraint, Arrays.toString(strings)));
                                    for(int i = 0; i < strings.length; i++) {
                                        //TODO should check constraints
                                        if (strings[i] == null) {
                                            // writer.writeNull();
                                        }
                                        else {
                                            writeString(new String((char[])strings[i]), (ListVector) vector, writer);
                                        }
                                    }
                                    break;
                                default:
                                    throw new IllegalArgumentException("unsupported kdbtypechar " + kdbtypechar);
                            }
                            writer.endList();
                            ((ListVector) vector).setNotNull(rowNum);
                        }
                        return true;
                };
                return fieldwriter;
            };
            return factory;
        }
        else
        {
            throw new IllegalArgumentException("unsupported fieldType " + fieldType + " field name " + fieldName);
        }
    }

    private static void writeString(String value, ListVector vector, UnionListWriter writer) {
        byte[] bytes = value.getBytes(Charsets.UTF_8);
        try (ArrowBuf buf = vector.getAllocator().buffer(bytes.length)) {
            buf.writeBytes(bytes);
            writer.varChar().writeVarChar(0, buf.readableBytes(), buf);
        }                                
    }

    @VisibleForTesting
    static String toVarChar(int[] a)
    {
        //StringBuilder with pre allocated is faster than Stream.
        StringBuilder sb = new StringBuilder(STRING_BUFFER_INITIAL_CAPACITY);
        for (int i = 0; i < a.length; i++)
        {
            if (i > 0)
                sb.append(" ");
            if (a[i] == Integer.MIN_VALUE) {
                sb.append("0Ni"); //null value
            } else {
                sb.append(a[i]);
            }
        }
        return sb.toString();
    }

    @VisibleForTesting
    static String toVarChar(long[] a)
    {
        //StringBuilder with pre allocated is faster than Stream.
        StringBuilder sb = new StringBuilder(STRING_BUFFER_INITIAL_CAPACITY);
        for (int i = 0; i < a.length; i++)
        {
            if (i > 0)
                sb.append(" ");
            if (a[i] == Long.MIN_VALUE) {
                sb.append("0Nj"); //null value
            } else {
                sb.append(a[i]);
            }
        }
        return sb.toString();
    }

    @VisibleForTesting
    static String toVarChar(double[] a)
    {
        //StringBuilder with pre allocated is faster than Stream.
        StringBuilder sb = new StringBuilder(STRING_BUFFER_INITIAL_CAPACITY);
        for (int i = 0; i < a.length; i++)
        {
            if (i > 0)
                sb.append(" ");
            sb.append(a[i]);
        }
        return sb.toString();
    }

    @VisibleForTesting
    static String toVarChar(byte[] a)
    {
        final char[] chars = Hex.encodeHex(a);
        StringBuilder s = new StringBuilder(chars.length + 2);
        s.append("0x");
        s.append(chars);
        return s.toString();
    }

    @VisibleForTesting
    static String toVarChar(Timestamp[] timestamps)
    {
        //StringBuilder with pre allocated is faster than Stream.
        StringBuilder sb = new StringBuilder(STRING_BUFFER_INITIAL_CAPACITY);
        for (int i = 0; i < timestamps.length; i++)
        {
            if (i > 0)
                sb.append(" ");
            if (timestamps[i].getTime() == Long.MIN_VALUE) {
                sb.append("0Np");
            } else {
                sb.append(KdbQueryStringBuilder.toLiteral(timestamps[i], Types.MinorType.VARCHAR, KdbTypes.timestamp_type));
            }
        }
        return sb.toString();
    }

    @VisibleForTesting
    static String toVarChar(String[] symbols)
    {
        //StringBuilder with pre allocated is faster than Stream.
        StringBuilder sb = new StringBuilder(STRING_BUFFER_INITIAL_CAPACITY);
        for (int i = 0; i < symbols.length; i++)
        {
            sb.append(KdbQueryStringBuilder.toLiteral(symbols[i], Types.MinorType.VARCHAR, KdbTypes.symbol_type));
        }
        return sb.toString();
    }
}
