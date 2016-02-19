package golan.hello.hadoop.mng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by golaniz on 15/02/2016.
 */
public class HdfsSimpleRead {

    public static void main(String[] args) throws IOException, URISyntaxException {

        String fqdn = "myd-vm22661.hpswlabs.adapps.hp.com";
        if (args.length>0) fqdn = args[0];
        readHadoopFS(fqdn);
    }


    protected static void readHadoopFS(String fqdn) throws IOException, URISyntaxException {
        String hdfsUrl = "hdfs://" + fqdn + ":9000/user/hadoop/ebooks/gutenberg_input.txt";

        FileSystem fs = FileSystem.get(new URI(hdfsUrl), new Configuration());
        Path pt=new Path(hdfsUrl);

        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
        int count = 0;
        int sum = 0;
        while (line != null){
            line=br.readLine();
            if (line!=null) {
                count++;
                sum += line.length();
            }
        }
        System.out.println("sum=["+sum+"] count=["+count+"] ");
    }
}
