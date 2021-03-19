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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker.Bound;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.connectors.athena.jdbc.manager.JdbcRecordHandler.SkipQueryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Extends {@link JdbcSplitQueryBuilder} and implements Kdb specific SQL clauses for split.
 *
 * Kdb provides named partitions which can be used in a FROM clause.
 */
public class KdbQueryStringBuilder
        extends JdbcSplitQueryBuilder
{
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(KdbQueryStringBuilder.class);
    private static final org.joda.time.LocalDateTime EPOCH = new org.joda.time.LocalDateTime(1970, 1, 1, 0, 0);

    public KdbQueryStringBuilder(final String quoteCharacters)
    {
        super(quoteCharacters);
    }

    /**
     * Common logic to build Split SQL including constraints translated in where clause.
     *
     * @param jdbcConnection JDBC connection. See {@link Connection}.
     * @param catalog Athena provided catalog name.
     * @param schema table schema name.
     * @param table table name.
     * @param tableSchema table schema (column and type information).
     * @param constraints constraints passed by Athena to push down.
     * @param split table split.
     * @return prepated statement with SQL. See {@link PreparedStatement}.
     * @throws SQLException JDBC database exception.
     */
    @Override
    public PreparedStatement buildSql(
            final Connection jdbcConnection,
            final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final Split split)
            throws SQLException
    {
        final String sql = buildSqlString(catalog, schema, table, tableSchema, constraints, split);
        PreparedStatement statement = jdbcConnection.prepareStatement(sql);

        return statement;
    }
      
    /**
     * Common logic to build Split SQL including constraints translated in where clause.
     *
     * @param catalog Athena provided catalog name.
     * @param schema table schema name.
     * @param table table name.
     * @param tableSchema table schema (column and type information).
     * @param constraints constraints passed by Athena to push down.
     * @param split table split.
     * @return prepated statement with SQL. See {@link PreparedStatement}.
     * @throws SQLException JDBC database exception.
     */
    @VisibleForTesting
    String buildSqlString(
            final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final Split split)
            throws SQLException
    {   
        LOGGER.info("buildSqlString catalog={} schema={}, table={}, tableSchema={} constraints={}, split={}"
            , catalog
            , schema
            , table
            , tableSchema
            , constraints
            , split
            );
        final String partition_name = split.getProperty(KdbMetadataHandler.PARTITION_COLUMN_NAME);
        LOGGER.info("partition_name={}", partition_name);
        int total_partitions = 1;
        int partition_idx = 0;
        if(partition_name.contains("/")) {
            String[] a = partition_name.split("/", 2);
            partition_idx = Integer.parseInt(a[0]) - 1;
            total_partitions = Integer.parseInt(a[1]);
        }
        
        StringBuilder sql = new StringBuilder();

        String columnNames = tableSchema.getFields().stream()
                .map(Field::getName)
                .filter(c -> !split.getProperties().containsKey(c))
                .map(this::quote)
                .collect(Collectors.joining(", "));

        sql.append("q) ");
        sql.append("select ");
        sql.append(columnNames);
        if (columnNames.isEmpty()) {
            sql.append("null");
        }

        //push down parition clauses
        final ValueSet date_valueset = (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) ? constraints.getSummary().get("date") : null;
        DateCriteria daterange = getDateRange(date_valueset);
        if(daterange == null)
        {
            if(partition_idx != 0)
                throw new SkipQueryException("no date range criteria. skipping query as partition_idx=" + partition_idx);
        }
        else
        {
            LOGGER.info("pushdownable date range criteria is found. " + daterange);
            daterange = getDateRangeParallelQuery(daterange, total_partitions, partition_idx);
            LOGGER.info("In the result of parallel query splitting, now date range criteria is " + daterange);
        }

        String kdbTableName = KdbMetadataHandler.athenaTableNameToKdbTableName(table);
        //push down date criteria
        if (daterange != null) {
            kdbTableName = pushDownDateCriteriaIntoFuncArgs(kdbTableName, daterange);
        }
        sql.append(" from " + quote(kdbTableName) + " ");

        List<TypeAndValue> accumulator = new ArrayList<>();
        List<String> clauses = new ArrayList<>();
        //use daterange
        if(daterange != null)
        {
            clauses.add("(date within (" + KdbQueryStringBuilder.toLiteral(daterange.from_day, MinorType.DATEDAY, null) + ";" + KdbQueryStringBuilder.toLiteral(daterange.to_day, MinorType.DATEDAY, null) + "))");
        }
        //normal where clauses
        clauses.addAll(toConjuncts(tableSchema.getFields(), constraints, accumulator, split.getProperties()));

        if (!clauses.isEmpty()) {
            sql.append(" where ")
                    .append(Joiner.on(" , ").join(clauses));
        }

        LOGGER.info("Generated SQL : {}", sql.toString());

        return sql.toString();
    }
    
    public static class DateCriteria {
        public final int from_day;
        public final int to_day;

        public DateCriteria(int from_day, int to_day)
        {
            this.from_day = from_day;
            this.to_day   = to_day;
        }
        
        public String getFromDate() {
            return KdbQueryStringBuilder.toLiteral(from_day, MinorType.DATEDAY, null);
        }
        
        public String getToDate() {
            return KdbQueryStringBuilder.toLiteral(to_day, MinorType.DATEDAY, null);
        }
        
        @Override
        public String toString() {
            return from_day + "(" + getFromDate() + ")-" + to_day + "(" + getToDate() + ")";
        }
    }

    static public DateCriteria getDateRange(ValueSet valueSet)
    {
        LOGGER.info("getDateRange valueset={}", valueSet);
        DateCriteria c = null;

        if(valueSet == null)
            return null; //no date range criteria
            
        if(valueSet.isNullAllowed())
            return null; //no date range criteria

        if (! (valueSet instanceof SortedRangeSet))
            return null; //no date range criteria

        Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
        if (rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded())
            return null;

        List<Range> ranges = valueSet.getRanges().getOrderedRanges();
        LOGGER.info("ranges={}", ranges);
        if(ranges.size() != 1)
            return null;
    
        Range range = ranges.get(0);
        if (range.isSingleValue())
        {
            return new DateCriteria((Integer)range.getLow().getValue(), (Integer)range.getLow().getValue());
        }
        else if (!range.getLow().isLowerUnbounded() && range.getLow().getBound() == Bound.EXACTLY && !range.getHigh().isUpperUnbounded() && range.getHigh().getBound() == Bound.EXACTLY) {
            return new DateCriteria((Integer)range.getLow().getValue(), (Integer)range.getHigh().getValue());
        }
        else
        {
            return null;
        }
    }
    
    static public DateCriteria getDateRangeParallelQuery(DateCriteria daterange, int total_partitions, int partition_idx)
    {
        int days = daterange.to_day - daterange.from_day + 1;
        int[] a = getDateRangeParallelQuery(days, total_partitions);
        LOGGER.info("days assignment {}", Arrays.toString(a));
        int fromday = daterange.from_day;
        for(int i = 0; i < total_partitions; i++)
        {
            if(i == partition_idx)
            {
                if(a[i] == 0)
                    throw new SkipQueryException("skip query as no assigned query at partition_idx=" + partition_idx);
                return new DateCriteria(fromday, fromday + a[i] - 1);
            }
            else
                fromday += a[i];
        }
        //we shouldn't come here.
        throw new RuntimeException("something wrong.");
    }
    
    static public int[] getDateRangeParallelQuery(int days, int total_partitions) {
        if(days < 1)
            throw new IllegalArgumentException("days should be positive but was " + days);
        if(total_partitions < 1)
            throw new IllegalArgumentException("total_partitions should be positive but was " + total_partitions);
        final int num_days_per_part = days / total_partitions;
        int[] a = new int[total_partitions];
        int n = 0;
        for(int i = 0; i < total_partitions; i++)
        {
            if(i == total_partitions - 1)
                a[i] = days - n;
            else
            {
                a[i] = num_days_per_part;
                n += num_days_per_part;
            }
        }
        return a;
    }

    static public String pushDownDateCriteriaIntoFuncArgs(String kdbTableName, DateCriteria daterange)
    {
        String from = KdbQueryStringBuilder.toLiteral(daterange.from_day, MinorType.DATEDAY, null);
        String to   = KdbQueryStringBuilder.toLiteral(daterange.to_day  , MinorType.DATEDAY, null);
        //First two arguments of function look date type and date_from and date_to are given.
        kdbTableName = kdbTableName.replaceFirst(
            "\\[ *[0-9][0-9][0-9][0-9]\\.[0-9][0-9]\\.[0-9][0-9] *; *[0-9][0-9][0-9][0-9]\\.[0-9][0-9]\\.[0-9][0-9] *"
            , "[" + from + ";" + to);

        //not supported
        // else if(date_from != null) {
        //     //First one argument  of function look date type and date_from.
        //     if(kdbTableName.matches(".*\\[ *[0-9][0-9][0-9][0-9]\\.[0-9][0-9]\\.[0-9][0-9] *; *[0-9][0-9][0-9][0-9]\\.[0-9][0-9]\\.[0-9][0-9].*")) {
        //         //if funciton has two dates arguments, we should skip it.
        //         //TESTED by test_select_stmt_func_subquery_where_pushdown5_only_from_clause
        //     }
        //     else {
        //         kdbTableName = kdbTableName.replaceFirst( "\\[ *[0-9][0-9][0-9][0-9]\\.[0-9][0-9]\\.[0-9][0-9] *", "[" + date_from);
        //     }
        // }
        return kdbTableName;
    }

    @Override
    protected String getFromClauseWithSplit(String catalog, String schema, String athenaTableName, Split split)
    {
        throw new RuntimeException("getFromClauseWithSplit is not used in kdb+");
    }

    @Override
    protected List<String> getPartitionWhereClauses(final Split split)
    {
        return Collections.emptyList();
    }

    private static final ThreadLocal<DateTimeFormatter> DATE_FORMAT = new ThreadLocal<DateTimeFormatter>() {
        @Override
        protected DateTimeFormatter initialValue()
        {
            return DateTimeFormat.forPattern("yyyy.MM.dd");
        }
    };

    private static final ThreadLocal<DateTimeFormatter> TIME_FORMAT = new ThreadLocal<DateTimeFormatter>()
    {
        @Override
        protected DateTimeFormatter initialValue()
        {
            return DateTimeFormat.forPattern("HH:mm:ss.SSS000000");
        }
    };

    private static final ThreadLocal<Function<Timestamp, String>> TIMESTAMP_FORMAT = new ThreadLocal<Function<Timestamp, String>>()
    {
        final SimpleDateFormat datetime_format = new SimpleDateFormat("yyyy.MM.dd'D'HH:mm:ss");
        final DecimalFormat nano_format = new DecimalFormat("000000000");
        @Override
        protected Function<Timestamp, String> initialValue()
        {
            return new Function<Timestamp, String>() {
                @Override
                public String apply(Timestamp value) {
                    return datetime_format.format(value) + "." + nano_format.format(value.getNanos());
                }
            };
        }
    };

    static String toLiteral(Object value, ArrowType type, String columnName, Field column)
    {        
        if(LOGGER.isDebugEnabled()) LOGGER.debug("column:" + String.valueOf(columnName) + " value:" + String.valueOf(value));
        String literal = toLiteral(value, Types.getMinorTypeForArrowType(type), KdbTypes.valueOf(column.getMetadata().get(KdbMetadataHandler.KDBTYPE_KEY)));
        return literal;
    }

    @VisibleForTesting
    static String toLiteral(Object value, Types.MinorType minorTypeForArrowType, KdbTypes kdbtype)
    {
        if(LOGGER.isDebugEnabled()) LOGGER.debug("kdbtype:" + String.valueOf(kdbtype) + " minorTypeForArrowType:" + String.valueOf(minorTypeForArrowType) + " value:" + String.valueOf(value) + (value == null ? "null" : value.getClass().getName()));
        final String literal = _toLiteral(value, minorTypeForArrowType, kdbtype);
        if(LOGGER.isDebugEnabled()) LOGGER.debug("literal:" + String.valueOf(literal));
        return literal;
    }

    static private String _toLiteral(Object value, Types.MinorType minorTypeForArrowType, KdbTypes kdbtype)
    {
        if(LOGGER.isDebugEnabled()) LOGGER.debug("minortype:" + String.valueOf(minorTypeForArrowType) + " kdbtype:" + String.valueOf(kdbtype) + " value:" + String.valueOf(value) + " valuetype:" + (value == null ? "null" : value.getClass().getName()));

        switch (minorTypeForArrowType) {
            case BIGINT:
                if (value == null) {
                    return "0Nj";
                }
                else {
                    return String.valueOf(value);
                }
            case INT:
                if (value == null) {
                    return "0Ni";
                }
                else {
                    return ((Number) value).intValue() + "i";
                }
            case SMALLINT:
                if (value == null) {
                    return "0Nh";
                }
                else {
                    return ((Number) value).shortValue() + "i";
                }
            case TINYINT: //byte
                if (value == null) {
                    return "0x00";
                }
                else {
                    return ((Number) value).byteValue() + "i";
                }
            case FLOAT8:
                if (kdbtype == KdbTypes.real_type) {
                    if (value == null) {
                        return "0Ne";
                    }
                    else {
                        return String.valueOf(((Number) value).doubleValue()) + "e"; 
                    }
                }
                else {
                    if (value == null) {
                        return "0n";
                    }
                    else {
                        return String.valueOf(((Number) value).doubleValue());
                    }
                }
            case FLOAT4: //real
                if (value == null) {
                    return "0Ne";
                }
                else {
                    return String.valueOf(((Number) value).floatValue());
                }
            case BIT: //boolean
                if (value == null) {
                    return "0b";
                }
                else {
                    return ((boolean) value) ? "1b" : "0b";
                }
            case DATEDAY:
                if (value == null) {
                    return "0Nd";
                }
                else {
                    if (value instanceof Number) {
                        final int days = ((Number) value).intValue();
                        final org.joda.time.LocalDateTime dateTime = EPOCH.minusDays(-days);
                        return DATE_FORMAT.get().print(dateTime);
                    }
                    else {
                        final org.joda.time.LocalDateTime dateTime = ((org.joda.time.LocalDateTime) value);
                        return DATE_FORMAT.get().print(dateTime);
                    }
                }
            case DATEMILLI:
                if (value == null) {
                    return "0Np";
                }
                else {
                    org.joda.time.LocalDateTime timestamp = ((org.joda.time.LocalDateTime) value);
                    return DATE_FORMAT.get().print(timestamp) + "D" + TIME_FORMAT.get().print(timestamp);
                }
            case VARCHAR:
                switch(kdbtype) {
                    case guid_type:
                        if (value == null) {
                            return "0Ng";
                        }
                        else {
                            return "\"G\"$\"" + value + "\"";
                        }
                    case char_type:
                        if (value == null) {
                            return "\" \"";
                        }
                        else {
                            return "\"" + value.toString() + "\"";
                        }
                    case time_type:
                        if (value == null) {
                            return "0Nt";
                        }
                        else {
                            return value.toString();
                        }
                    case timespan_type:
                        if (value == null) {
                            return "0Nn";
                        }
                        else {
                            return value.toString();
                        }
                    case timestamp_type:
                        if (value == null) {
                            return "0Np";
                        }
                        else {
                            if (value instanceof Timestamp) {
                                final Timestamp timestamp = (Timestamp) value;
                                if(LOGGER.isDebugEnabled()) LOGGER.debug(String.format("timestamp#getTime:%s, getNanos:%s", timestamp.getTime(), timestamp.getNanos()));
                                return TIMESTAMP_FORMAT.get().apply(timestamp);
                            }
                            else {
                                return value.toString();
                            }
                        }                        
                    case list_of_char_type:
                        throw new UnsupportedOperationException("list of char type cannot be pushed down to where statement");
                    default:
                        //symbol
                        if (value == null) {
                            return "` ";
                        }
                        else {
                            return "`" + String.valueOf(value);
                        }                        
                }
            // case VARBINARY:
            //     return String.valueOf((byte[]) typeAndValue.getValue()); //or throw exception
            // case DECIMAL:
            //     ArrowType.Decimal decimalType = (ArrowType.Decimal) type;
            //     BigDecimal decimal = BigDecimal.valueOf((Long) value, decimalType.getScale());
            //     return decimal.toPlainString();
            default:
                throw new UnsupportedOperationException(String.format("Can't handle type: %s", minorTypeForArrowType));
        }
    }

    @Override
    protected List<String> toConjuncts(List<Field> columns, Constraints constraints, List<TypeAndValue> accumulator, Map<String, String> partitionSplit)
    {
        List<String> conjuncts = new ArrayList<>();
        for (Field column : columns) {
            if (partitionSplit.containsKey(column.getName())) {
                continue; // Ignore constraints on partition name as RDBMS does not contain these as columns. Presto will filter these values.
            }
            final char kdbtype = KdbMetadataHandler.getKdbTypeChar(column);
            switch(kdbtype) {
                case 'C': //list of char
                case 'P': //list of timestamp
                case 'S': //list of symbol
                case 'X': //list of byte
                case 'H': //list of short
                case 'I': //list of int
                case 'J': //list of long
                case 'E': //list of real
                case 'F': //list of float
                case 'B': //list of bit
                case 'G': //list of guid
                case 'D': //list of date
                    LOGGER.info("list column is excluded from where caluse. columnName=" + column.getName());
                    continue;
                default:
                    //no default logic
            }
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    conjuncts.add(toPredicate(column.getName(), column, valueSet, type, accumulator));
                }
            }
        }
        return conjuncts;
    }

    protected String toPredicate(String columnName, Field column, ValueSet valueSet, ArrowType type, List<TypeAndValue> accumulator)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        // TODO Add isNone and isAll checks once we have data on nullability.

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return toPredicateNull(columnName, column, type, accumulator);
            }

            // we don't need to add disjunction(OR (colname IS NULL)) because
            if (valueSet.isNullAllowed()) {
                disjuncts.add(toPredicateNull(columnName, column, type, accumulator));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return toPredicateNull(columnName, column, type, accumulator);
            }

            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded() && range.getLow().getBound() == Bound.EXACTLY && !range.getHigh().isUpperUnbounded() && range.getHigh().getBound() == Bound.EXACTLY) {
                        //between = within
                        rangeConjuncts.add(quote(columnName) + " within (" + toLiteral(range.getLow().getValue(), type, columnName, column) + ";" + toLiteral(range.getHigh().getValue(), type, columnName, column) + ")");
                    }
                    else
                    {
                        if (!range.getLow().isLowerUnbounded()) {
                            switch (range.getLow().getBound()) {
                                case ABOVE:
                                    rangeConjuncts.add(toPredicate(columnName, column, ">", range.getLow().getValue(), type, accumulator));
                                    break;
                                case EXACTLY:
                                    rangeConjuncts.add(toPredicate(columnName, column, ">=", range.getLow().getValue(), type, accumulator));
                                    break;
                                case BELOW:
                                    throw new IllegalArgumentException("Low marker should never use BELOW bound");
                                default:
                                    throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                            }
                        }
                        if (!range.getHigh().isUpperUnbounded()) {
                            switch (range.getHigh().getBound()) {
                                case ABOVE:
                                    throw new IllegalArgumentException("High marker should never use ABOVE bound");
                                case EXACTLY:
                                    rangeConjuncts.add(toPredicate(columnName, column, "<=", range.getHigh().getValue(), type, accumulator));
                                    break;
                                case BELOW:
                                    rangeConjuncts.add(toPredicate(columnName, column, "<", range.getHigh().getValue(), type, accumulator));
                                    break;
                                default:
                                    throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                            }
                        }
                    }
                    // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    StringBuilder rngsql = new StringBuilder();
                    for (int i = 0; i < rangeConjuncts.size(); i++) {
                        if (i > 0)
                            rngsql.append(" and ");
                        if (rangeConjuncts.size() > 0)
                            rngsql.append("(");
                        rngsql.append(rangeConjuncts.get(i));
                        if (rangeConjuncts.size() > 0)
                            rngsql.append(")");
                    }
                    disjuncts.add(rngsql.toString());
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicate(columnName, column, "=", Iterables.getOnlyElement(singleValues), type, accumulator));
            }
            else if (singleValues.size() > 1) {
                final StringBuilder insql = new StringBuilder();
                insql.append(quote(columnName));
                insql.append(" in (");
                int count = 0;
                for (Object val : singleValues) {
                    if (count > 0)
                        insql.append(", ");
                    insql.append(toLiteral(val, type, columnName, column));
                    count++;
                }
                insql.append(")");
                disjuncts.add(insql.toString());
            }
        }

        return "(" + Joiner.on(" or ").join(disjuncts) + ")";
    }

    protected String toPredicateNull(String columnName, Field column, ArrowType type, List<TypeAndValue> accumulator)
    {
        // accumulator.add(new TypeAndValue(type, value));
        return "(" + quote(columnName) + " = " + toLiteral(null, type, columnName, column) + ")";
    }

    protected String toPredicate(String columnName, Field column, String operator, Object value, ArrowType type, List<TypeAndValue> accumulator)
    {
        // accumulator.add(new TypeAndValue(type, value));
        return quote(columnName) + " " + operator + " " + toLiteral(value, type, columnName, column);
    }

    @Override
    protected String quote(String name)
    {
        return name;
    }
}
