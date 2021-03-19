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

public enum KdbTypes
{
    bit_type('b'),
    byte_type('x'),
    char_type('c'),
    short_type('h'),
    int_type('i'),
    long_type('j'),
    list_of_long_type('J'),
    list_of_int_type('I'),
    list_of_byte_type('X'),
    list_of_float_type('F'),
    list_of_symbol_type('S'),
    list_of_timestamp_type('P'),
    list_of_list_of_char_type('V'),
    real_type('r'),
    float_type('f'),
    date_type('d'),
    time_type('t'),
    timespan_type('n'),
    timestamp_type('p'),
    guid_type('g'),
    symbol_type('s'),
    list_of_char_type('C');

    public final char kdbtype;
    
    private KdbTypes(char kdbtype)
    {
        this.kdbtype = kdbtype;
    }
}
