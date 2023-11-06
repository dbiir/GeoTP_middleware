/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.sql.parser.sql.common.util;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.sql.parser.api.visitor.ASTNode;
import org.apache.shardingsphere.sql.parser.sql.common.enums.Paren;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.complex.CommonExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.JoinTableSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.SubqueryTableSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.TableSegment;
import org.apache.shardingsphere.sql.parser.sql.common.value.literal.impl.BooleanLiteralValue;
import org.apache.shardingsphere.sql.parser.sql.common.value.literal.impl.NullLiteralValue;
import org.apache.shardingsphere.sql.parser.sql.common.value.literal.impl.NumberLiteralValue;
import org.apache.shardingsphere.sql.parser.sql.common.value.literal.impl.OtherLiteralValue;
import org.apache.shardingsphere.sql.parser.sql.common.value.literal.impl.StringLiteralValue;

import javax.transaction.xa.Xid;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * SQL utility class.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SQLUtils {
    
    private static final String SQL_END = ";";
    
    private static final String COMMENT_PREFIX = "/*";
    
    private static final String COMMENT_SUFFIX = "*/";
    
    private static final String EXCLUDED_CHARACTERS = "[]`'\"";
    
    private static final Pattern SINGLE_CHARACTER_PATTERN = Pattern.compile("^_|([^\\\\])_");
    
    private static final Pattern SINGLE_CHARACTER_ESCAPE_PATTERN = Pattern.compile("\\\\_");
    
    private static final Pattern ANY_CHARACTER_PATTERN = Pattern.compile("^%|([^\\\\])%");
    
    private static final Pattern ANY_CHARACTER_ESCAPE_PATTERN = Pattern.compile("\\\\%");
    
    private static final char[] HEX_DIGITS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    
    /**
     * Get exactly number value and type.
     *
     * @param value string to be converted
     * @param radix radix
     * @return exactly number value and type
     */
    public static Number getExactlyNumber(final String value, final int radix) {
        try {
            return getBigInteger(value, radix);
        } catch (final NumberFormatException ex) {
            return new BigDecimal(value);
        }
    }
    
    private static Number getBigInteger(final String value, final int radix) {
        BigInteger result = new BigInteger(value, radix);
        if (result.compareTo(new BigInteger(String.valueOf(Integer.MIN_VALUE))) >= 0 && result.compareTo(new BigInteger(String.valueOf(Integer.MAX_VALUE))) <= 0) {
            return result.intValue();
        }
        if (result.compareTo(new BigInteger(String.valueOf(Long.MIN_VALUE))) >= 0 && result.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) <= 0) {
            return result.longValue();
        }
        return result;
    }
    
    /**
     * Get exactly value for SQL expression.
     * 
     * <p>remove special char for SQL expression</p>
     * 
     * @param value SQL expression
     * @return exactly SQL expression
     */
    public static String getExactlyValue(final String value) {
        return null == value ? null : CharMatcher.anyOf(EXCLUDED_CHARACTERS).removeFrom(value);
    }
    
    /**
     * Get exactly value for SQL expression.
     *
     * <p>remove special char for SQL expression</p>
     *
     * @param value SQL expression
     * @param reservedCharacters characters to be reserved
     * @return exactly SQL expression
     */
    public static String getExactlyValue(final String value, final String reservedCharacters) {
        if (null == value) {
            return null;
        }
        String toBeExcludedCharacters = CharMatcher.anyOf(reservedCharacters).removeFrom(EXCLUDED_CHARACTERS);
        return CharMatcher.anyOf(toBeExcludedCharacters).removeFrom(value);
    }
    
    /**
     * Get exactly SQL expression.
     *
     * <p>remove space for SQL expression</p>
     *
     * @param value SQL expression
     * @return exactly SQL expression
     */
    public static String getExactlyExpression(final String value) {
        return Strings.isNullOrEmpty(value) ? value : CharMatcher.anyOf(" ").removeFrom(value);
    }
    
    /**
     * Get exactly SQL expression without outside parentheses.
     * 
     * @param value SQL expression
     * @return exactly SQL expression
     */
    public static String getExpressionWithoutOutsideParentheses(final String value) {
        int parenthesesOffset = getParenthesesOffset(value);
        return 0 == parenthesesOffset ? value : value.substring(parenthesesOffset, value.length() - parenthesesOffset);
    }
    
    private static int getParenthesesOffset(final String value) {
        int result = 0;
        if (Strings.isNullOrEmpty(value)) {
            return result;
        }
        while (Paren.PARENTHESES.getLeftParen() == value.charAt(result)) {
            result++;
        }
        return result;
    }
    
    /**
     * Get subquery from tableSegment.
     *
     * @param tableSegment TableSegment
     * @return exactly SubqueryTableSegment list
     */
    public static List<SubqueryTableSegment> getSubqueryTableSegmentFromTableSegment(final TableSegment tableSegment) {
        List<SubqueryTableSegment> result = new LinkedList<>();
        if (tableSegment instanceof SubqueryTableSegment) {
            result.add((SubqueryTableSegment) tableSegment);
        }
        if (tableSegment instanceof JoinTableSegment) {
            result.addAll(getSubqueryTableSegmentFromJoinTableSegment((JoinTableSegment) tableSegment));
        }
        return result;
    }
    
    private static List<SubqueryTableSegment> getSubqueryTableSegmentFromJoinTableSegment(final JoinTableSegment joinTableSegment) {
        List<SubqueryTableSegment> result = new LinkedList<>();
        if (joinTableSegment.getLeft() instanceof SubqueryTableSegment) {
            result.add((SubqueryTableSegment) joinTableSegment.getLeft());
        } else if (joinTableSegment.getLeft() instanceof JoinTableSegment) {
            result.addAll(getSubqueryTableSegmentFromJoinTableSegment((JoinTableSegment) joinTableSegment.getLeft()));
        }
        if (joinTableSegment.getRight() instanceof SubqueryTableSegment) {
            result.add((SubqueryTableSegment) joinTableSegment.getRight());
        } else if (joinTableSegment.getRight() instanceof JoinTableSegment) {
            result.addAll(getSubqueryTableSegmentFromJoinTableSegment((JoinTableSegment) joinTableSegment.getRight()));
        }
        return result;
    }
    
    /**
     * Create literal expression.
     * 
     * @param astNode AST node
     * @param startIndex start index
     * @param stopIndex stop index
     * @param text text
     * @return literal expression segment
     */
    public static ExpressionSegment createLiteralExpression(final ASTNode astNode, final int startIndex, final int stopIndex, final String text) {
        if (astNode instanceof StringLiteralValue) {
            return new LiteralExpressionSegment(startIndex, stopIndex, ((StringLiteralValue) astNode).getValue());
        }
        if (astNode instanceof NumberLiteralValue) {
            return new LiteralExpressionSegment(startIndex, stopIndex, ((NumberLiteralValue) astNode).getValue());
        }
        if (astNode instanceof BooleanLiteralValue) {
            return new LiteralExpressionSegment(startIndex, stopIndex, ((BooleanLiteralValue) astNode).getValue());
        }
        if (astNode instanceof NullLiteralValue) {
            return new LiteralExpressionSegment(startIndex, stopIndex, null);
        }
        if (astNode instanceof OtherLiteralValue) {
            return new CommonExpressionSegment(startIndex, stopIndex, ((OtherLiteralValue) astNode).getValue());
        }
        return new CommonExpressionSegment(startIndex, stopIndex, text);
    }
    
    /**
     * Trim the semicolon of SQL.
     *
     * @param sql SQL to be trim
     * @return SQL without semicolon
     */
    public static String trimSemicolon(final String sql) {
        return sql.endsWith(SQL_END) ? sql.substring(0, sql.length() - 1) : sql;
    }
    
    /**
     * Trim the comment of SQL.
     *
     * @param sql SQL to be trim
     * @return remove comment from SQL
     */
    public static String trimComment(final String sql) {
        String result = sql;
        if (sql.startsWith(COMMENT_PREFIX)) {
            result = result.substring(sql.indexOf(COMMENT_SUFFIX) + 2);
        }
        if (sql.endsWith(SQL_END)) {
            result = result.substring(0, result.length() - 1);
        }
        return result.trim();
    }
    
    /**
     * Convert like pattern to regex.
     * 
     * @param pattern like pattern
     * @return regex
     */
    public static String convertLikePatternToRegex(final String pattern) {
        String result = pattern;
        if (pattern.contains("_")) {
            result = SINGLE_CHARACTER_PATTERN.matcher(result).replaceAll("$1.");
            result = SINGLE_CHARACTER_ESCAPE_PATTERN.matcher(result).replaceAll("_");
        }
        if (pattern.contains("%")) {
            result = ANY_CHARACTER_PATTERN.matcher(result).replaceAll("$1.*");
            result = ANY_CHARACTER_ESCAPE_PATTERN.matcher(result).replaceAll("%");
        }
        return result;
    }
    
    /**
     * Split sql into multiple sqls
     *
     * @param sql Raw sql
     * @return split input sql into list of sqls (based on semicolon)
     */
    public static List<String> splitMultiSQL(final String sql) {
        List<String> result = new LinkedList<>();
        String[] strings = sql.split(";");
        
        for (String each : strings) {
            result.add(trimComment(each));
        }
        
        return result;
    }
    
    private static String getComment(final String sql) {
        String result = "";
        if (sql.startsWith(COMMENT_PREFIX)) {
            result = sql.substring(2, sql.indexOf(COMMENT_SUFFIX));
        }
        
        return result;
    }
    
    public static boolean isLastQuery(final String sql) {
        String comment = getComment(sql);
        return comment.contains("last query");
    }
    
    public static void appendAsHex(StringBuilder builder, int value) {
        if (value == 0) {
            builder.append("0x0");
        } else {
            int shift = 32;
            boolean nonZeroFound = false;
            builder.append("0x");
            
            do {
                shift -= 4;
                byte nibble = (byte) (value >>> shift & 15);
                if (nonZeroFound) {
                    builder.append(HEX_DIGITS[nibble]);
                } else if (nibble != 0) {
                    builder.append(HEX_DIGITS[nibble]);
                    nonZeroFound = true;
                }
            } while (shift != 0);
        }
    }
    
    public static void appendAsHex(StringBuilder builder, byte[] bytes) {
        builder.append("0x");
        
        for (byte b : bytes) {
            builder.append(HEX_DIGITS[b >>> 4 & 15]).append(HEX_DIGITS[b & 15]);
        }
    }
    
    public static String xidToHex(Xid xid) {
        StringBuilder builder = new StringBuilder();
        byte[] gtrid = xid.getGlobalTransactionId();
        byte[] btrid = xid.getBranchQualifier();
        if (gtrid != null) {
            SQLUtils.appendAsHex(builder, gtrid);
        }
        
        builder.append(',');
        if (btrid != null) {
            SQLUtils.appendAsHex(builder, btrid);
        }
        
        builder.append(',');
        SQLUtils.appendAsHex(builder, xid.getFormatId());
        
        return builder.toString();
    }
}
