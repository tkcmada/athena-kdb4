/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.connectors.athena.jdbc;

import com.amazonaws.connectors.athena.jdbc.kdb.TimeManager;
import com.google.common.base.Preconditions;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

public class TimeManagerRealtime implements TimeManager
{
    @Override
    public LocalDateTime newLocalDateTime(DateTimeZone zone)
    {
        Preconditions.checkArgument(zone != null, "zone is null");
        return new LocalDateTime(zone);
    }    
}
