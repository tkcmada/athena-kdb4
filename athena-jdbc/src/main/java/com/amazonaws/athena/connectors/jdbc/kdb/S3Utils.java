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

public final class S3Utils {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(S3Utils.class);

    private S3Utils() {}

    public static List<String> getLinesFromS3(String region, String bucketName, String[] keys) throws IOException
    {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .build();
        List<String> lines = new ArrayList<>();
        for(String key : keys)
        {
            LOGGER.info("bucket={}, key={}", bucketName, key);
            try(BufferedReader r = new BufferedReader(new InputStreamReader(s3.getObject(bucketName, key).getObjectContent(), "UTF-8"))) //o.getObjectContent() returns S3ObjectInputStream
            {
                String line;
                while((line = r.readLine()) != null)
                {
                    if(! line.isEmpty())
                    lines.add(line);
                }
            }
        }
        LOGGER.info("got list from S3. lines={}", lines);
        return lines;
    }
}
