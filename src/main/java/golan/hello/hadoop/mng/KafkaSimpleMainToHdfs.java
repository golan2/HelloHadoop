package golan.hello.hadoop.mng;

import com.jcraft.jsch.JSchException;
import golan.hello.hadoop.utils.CmdOpts;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class KafkaSimpleMainToHdfs  {
    private static final String  CLA_CONSUMER_GROUP     = "consumerGroup";
    private static final String  CLA_KAFKA_BROKERS_LIST = "kafkaBrokersList";
    private static final String  CLA_KAFKA_OFFSET       = "kafkaOffset";
    private static final String  CLA_KAFKA_TOPIC        = "kafkaTopic";
    private static final String  CLA_MAX_ITERATIONS     = "maxIterations";
    private static final String  CLA_HDFS_OUTPUT_PATH   = "hdfsOutputPath";
    private static final int     PARTITION              =     0;
    private static final int     CHUNK_SIZE             = 200;

    private final CmdOpts cmdOpts;
    private final String  clientId;

    public KafkaSimpleMainToHdfs(String[] args) throws ParseException {
        this.cmdOpts = new CmdOpts(args, getParamsMap(), Collections.emptySet());
        this.clientId = cmdOpts.get(CLA_CONSUMER_GROUP) + this.hashCode();
    }

    protected Map<String, String> getParamsMap() {
        HashMap<String, String> result = new HashMap<>();
        result.put(CLA_CONSUMER_GROUP, "ConsumerGroup_Izik");
        result.put(CLA_KAFKA_OFFSET, "0");
        result.put(CLA_KAFKA_TOPIC, "test1");
        result.put(CLA_MAX_ITERATIONS, "10000");
        result.put(CLA_KAFKA_BROKERS_LIST, "localhost"/*"myd-vm23458.hpswlabs.adapps.hp.com"*/);
        result.put(CLA_HDFS_OUTPUT_PATH, "/data_in/irisLibSVM.data");
        return result;
    }

    public static void main(String[] args) throws ParseException, InterruptedException, JSchException {
        KafkaSimpleMainToHdfs runner = new KafkaSimpleMainToHdfs(args);
        runner.run();
    }

    private void run() {
        try {
            SimpleConsumer consumer = new SimpleConsumer(cmdOpts.get(CLA_KAFKA_BROKERS_LIST), 9092, 100000, 64 * 1024, clientId);
            long offset = Long.parseLong(cmdOpts.get(CLA_KAFKA_OFFSET));
            boolean readMore = true;
            int chunks = 0;
            System.out.println("!!run - BEGIN");
            while (readMore) {
                FetchResponse fetchResponse = fetchNextChunk(consumer, offset);
                offset = processChunk(fetchResponse);
                long highWatermark = fetchResponse.highWatermark(cmdOpts.get(CLA_KAFKA_TOPIC), PARTITION);
                readMore = ++chunks < Integer.parseInt(cmdOpts.get(CLA_MAX_ITERATIONS)) && offset < highWatermark && !fetchResponse.hasError();
            }
            System.out.println("!!run - END chunks=["+chunks+"]");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    protected FetchResponse fetchNextChunk(SimpleConsumer consumer, long offset) {
        FetchRequest fetchRequest =  new FetchRequestBuilder()
                .clientId(clientId)
                .addFetch(cmdOpts.get(CLA_KAFKA_TOPIC), PARTITION, offset, CHUNK_SIZE)
                .build();
        return consumer.fetch(fetchRequest);
    }

    protected long processChunk(FetchResponse fetchResponse) throws IOException {
        FileSystem fileSystem = null;
        FSDataOutputStream outputStream = null;
        boolean consoleOnly = cmdOpts.get(CLA_HDFS_OUTPUT_PATH).equals("consoleOnly");
        try {
            long lastOffset = -1;
            for (MessageAndOffset mao :  fetchResponse.messageSet(cmdOpts.get(CLA_KAFKA_TOPIC), PARTITION)) {
                ByteBuffer payload = mao.message().payload();
                CharBuffer message = StandardCharsets.UTF_8.decode(payload);
                System.out.println("!!\tMessage ["+mao.offset()+"]: [" + message + "]");
                lastOffset = mao.nextOffset();
                if (!consoleOnly) {
                    if (fileSystem == null) fileSystem = openFileSystemConnection();
                    if (outputStream == null) outputStream = fileSystem.create(new Path(cmdOpts.get(CLA_HDFS_OUTPUT_PATH)));
                    outputStream.write((message.toString() + "\n").getBytes());
                }
            }
            return lastOffset;
        } finally {
            if (fileSystem != null) {
                fileSystem.close();
            }
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    protected static FileSystem openFileSystemConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://myd-vm22661.hpswlabs.adapps.hp.com:9000");
        conf.set("dfs.replication", "1");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return FileSystem.get(conf);
    }

}
