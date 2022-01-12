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
package com.amazonaws.athena.connectors.jdbc.kdb;

// snippet-start:[s3.java2.getobjectdata.import]
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// snippet-end:[s3.java2.getobjectdata.import]

public class S3FunctionResolver implements FunctionResolver
{
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(S3FunctionResolver.class);

    private final String region;
    private final String bucketName;
    private final String[] keys;
    
    
    public S3FunctionResolver(String region, String bucketName, String keys)
    {
        this.region = region;
        this.bucketName = bucketName;
        this.keys = keys.split(",");
        LOGGER.info("initializing S3FunctionResolver region={}, bucket={}, keys={}", region, this.bucketName, Arrays.toString(this.keys));
    }

    @Override
    public List<String> getKdbFunctionList() throws IOException
    {
        LOGGER.info("getting function list from S3...");
        return S3Utils.getLinesFromS3(region, bucketName, keys);
    }
}
