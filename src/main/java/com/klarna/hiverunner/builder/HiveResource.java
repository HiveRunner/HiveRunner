/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.klarna.hiverunner.builder;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Representation of a resource configuration
 */
class HiveResource {
    private final String targetFile;
    private final ByteArrayOutputStream byteArrayOutputStream;

    HiveResource(String targetFile) throws IOException {
        this(targetFile, new ByteArrayOutputStream());
    }

    HiveResource(String targetFile, Path dataFile) throws IOException {
        this(targetFile, createOutputStream(Files.readAllBytes(dataFile)));
    }

    HiveResource(String targetFile, String data) throws IOException {
        this(targetFile, createOutputStream(data.getBytes()));
    }

    private HiveResource(String targetFile, ByteArrayOutputStream byteArrayOutputStream) {
        this.targetFile = targetFile;
        this.byteArrayOutputStream = byteArrayOutputStream;
    }

    private static ByteArrayOutputStream createOutputStream(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(data);
        baos.close();
        return baos;
    }

    String getTargetFile() {
        return targetFile;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    public ByteArrayOutputStream getOutputStream() {
        return byteArrayOutputStream;
    }

}
