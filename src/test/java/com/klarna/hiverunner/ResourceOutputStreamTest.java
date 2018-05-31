/**
 * Copyright (C) 2013-2018 Klarna AB
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
package com.klarna.hiverunner;

import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

@RunWith(StandaloneHiveRunner.class)
public class ResourceOutputStreamTest {

    @HiveSQL(files = {}, autoStart = false)
    private HiveShell shell;

    @Test(expected = IllegalStateException.class)
    public void writeShouldOnlyBeAllowedBeforeStartHasBeenCalled() throws IOException {

        OutputStream resourceOutputStream =
                shell.getResourceOutputStream("${hiveconf:hadoop.tmp.dir}/baz/foo.bar");

        shell.start();

        resourceOutputStream.write("Foo\nBar\nBaz".getBytes());
    }

    @Test
    public void itShouldBePossibleToAddAResourceByOutputStream() throws IOException {

        OutputStream resourceOutputStream =
                shell.getResourceOutputStream("${hiveconf:hadoop.tmp.dir}/baz/foo.bar");

        resourceOutputStream.write("Foo\nBar\nBaz".getBytes());

        shell.addSetupScript("" +
                "create table foobar(str string) " +
                "location '${hiveconf:hadoop.tmp.dir}/baz'");

        shell.start();

        Assert.assertEquals(Arrays.asList("Foo", "Bar", "Baz"), shell.executeQuery("select * from foobar"));
    }

    @Test
    public void sequenceFile() throws IOException {

        OutputStream resourceOutputStream =
                shell.getResourceOutputStream("${hiveconf:hadoop.tmp.dir}/baz/foo.bar");

        SequenceFile.Writer sequenceFileWriter = createSequenceFileWriter(resourceOutputStream);

        sequenceFileWriter.append(NullWritable.get(), new Text("Foo"));
        sequenceFileWriter.append(NullWritable.get(), new Text("Bar"));
        sequenceFileWriter.append(NullWritable.get(), new Text("\\N"));
        sequenceFileWriter.append(NullWritable.get(), new Text("Baz"));

        shell.addSetupScript("" +
                "create table foobar(str string) " +
                "STORED AS SEQUENCEFILE " +
                "location '${hiveconf:hadoop.tmp.dir}/baz'");

        shell.start();

        Assert.assertEquals(Arrays.asList("Foo", "Bar", "_NULL_", "Baz"),
                shell.executeQuery("select * from foobar", "\t", "_NULL_"));
    }


    private SequenceFile.Writer createSequenceFileWriter(OutputStream resourceOutputStream) throws IOException {
        return SequenceFile.createWriter(new Configuration(),
                SequenceFile.Writer.stream(new FSDataOutputStream(resourceOutputStream, null)),
                SequenceFile.Writer.keyClass(NullWritable.class),
                SequenceFile.Writer.valueClass(Text.class));
    }

}
