package hk.ust.comp4651;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;

/**
 * Provides a FileSystem instance with Hadoop configs loaded from HADOOP_CONF_DIR.
 * Do NOT modify this class for the assignment.
 */
public class HDFSUtils {
    public static FileSystem getFileSystem() throws IOException {
        // By default, Configuration loads core-site.xml / hdfs-site.xml from HADOOP_CONF_DIR.
        Configuration conf = new Configuration();
        return FileSystem.get(conf);
    }
}