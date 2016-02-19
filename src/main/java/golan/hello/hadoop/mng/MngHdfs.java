package golan.hello.hadoop.mng;

import golan.hello.hadoop.utils.CmdOpts;
import golan.hello.hadoop.utils.SSHConnection;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.jcraft.jsch.Session;

/**
 * Operations to manage HDFS
 * (List files, read, write, etc...)
 */
public class MngHdfs {


    private static final boolean useSSH    = true;
    public  static final String OP_DELETE = "delete";
    public  static final String  OP_APPEND = "append";
    public  static final String  OP_WRITE  = "write";
    public  static final String  OP_READ   = "read";
    private static final String  OP_LIST   = "list";

    public static void main(String[] args) throws Exception {
        Session sshSession = null;
        FileSystem fileSystem = null;
        CmdOpts opts = new CmdOpts(args);

        try {

            // Start SSH tunneling to the HDFS
            if (useSSH) sshSession = SSHConnection.openSshSession(opts);

            // Connect to the HDFS
            fileSystem = openFileSystemConnection();

            // Do your thing
            Path path = new Path(opts.getPath());


            if (OP_DELETE.equals(opts.getOperation())) {
                deleteOperation(fileSystem, path);
            }
            else if (OP_APPEND.equals(opts.getOperation())) {
                appendOperation(fileSystem, path);
            }
            else if (OP_WRITE.equals(opts.getOperation())) {
                writeOperation(fileSystem, path);
            }
            else if (OP_READ.equals(opts.getOperation())) {
                readOpeartion(fileSystem, path);
            }
            else if (OP_LIST.equals(opts.getOperation())) {
                FileStatus[] status = fileSystem.listStatus(new Path("/user/hadoop/logs/logs1"));
                for (FileStatus s: status) {
                    System.out.println("File: " + s.getPath() + "\t\t\t Size: " + s.getLen() + " Bytes");
                }
            }

            else {
                throw new IllegalArgumentException("Unknown operation ["+opts.getOperation()+"]");
            }

        } finally {
            if (fileSystem != null) {
                System.out.println("Closing file connection...");
                fileSystem.close();
            }

            if (sshSession != null && sshSession.isConnected()) {
                System.out.println("Closing ssh connection...");
                sshSession.disconnect();
            }
        }

        System.out.println("DONE!");


    }

    protected static void readOpeartion(FileSystem fileSystem, Path path) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        String line;
        int numLines = 10;
        System.out.println("\nContents of file [" + path.toString() + "] up to " + numLines + " lines:");
        System.out.println("=============================================");
        for (int i = 0; (line = bufferedReader.readLine()) != null && i < numLines; i++) {
            System.out.println(line);
        }
        System.out.println("=============================================");
        bufferedReader.close();
    }

    protected static void writeOperation(FileSystem fileSystem, Path path) throws IOException {
        BufferedWriter bufferedWriter =
                new BufferedWriter(new OutputStreamWriter(fileSystem.create(path), "UTF-8"));
        bufferedWriter.write(getCurrentTimestamp() + " Hello Hadoop\n");
        bufferedWriter.close();
    }

    protected static void appendOperation(FileSystem fileSystem, Path path) throws IOException {
        BufferedWriter bufferedWriter =
                new BufferedWriter(new OutputStreamWriter(fileSystem.append(path), "UTF-8"));
        String str = getCurrentTimestamp() + " Hello again\n";
        bufferedWriter.append(str);
        bufferedWriter.close();
    }

    protected static void deleteOperation(FileSystem fileSystem, Path path) throws IOException {
        fileSystem.delete(path, true);
        if (fileSystem.exists(path)) {
            System.out.println("Failed to delete file");
        } else {
            System.out.println("File deleted successfully");
        }
    }

    protected static FileSystem openFileSystemConnection() throws IOException {
        FileSystem fileSystem;Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.replication", "1");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        fileSystem = FileSystem.get(conf);
        return fileSystem;
    }

    private static void closeConnections(Session sshSession) throws IOException {

    }

    private static String getCurrentTimestamp() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }

}
