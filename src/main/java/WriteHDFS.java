/**
 * Created by mjohnson on 2/2/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WriteHDFS extends Configured implements Tool{
    public static final String FS_PARAM_NAME = "fs.defaultFS";

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://HDPNN:9000");
        FileSystem fs = FileSystem.get(conf);
        String outputFile = "/user/hive/outputData.txt";
        FSDataOutputStream os = fs.create(new Path(outputFile));

        os.writeChars("This is an output test.");
        return 0;
    }

    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new WriteHDFS(), args);
        System.exit(returnCode);
    }

}

