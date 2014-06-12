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

        appendToSequenceFile(sequenceFileWriter, "Foo", "Bar", "Baz");

        shell.addSetupScript("" +
                "create table foobar(str string) " +
                "STORED AS SEQUENCEFILE " +
                "location '${hiveconf:hadoop.tmp.dir}/baz'");

        shell.start();

        Assert.assertEquals(Arrays.asList("Foo", "Bar", "Baz"), shell.executeQuery("select * from foobar"));
    }

    private void appendToSequenceFile(SequenceFile.Writer sequenceFileWriter, String... payloads) throws IOException {
        for (String payload : payloads) {
            sequenceFileWriter.append(NullWritable.get(), new Text(payload));
        }
    }

    private SequenceFile.Writer createSequenceFileWriter(OutputStream resourceOutputStream) throws IOException {
        return SequenceFile.createWriter(
                    new Configuration(),
                    new FSDataOutputStream(resourceOutputStream),
                    NullWritable.class,
                    Text.class,
                    SequenceFile.CompressionType.NONE,
                    null
            );
    }
}
