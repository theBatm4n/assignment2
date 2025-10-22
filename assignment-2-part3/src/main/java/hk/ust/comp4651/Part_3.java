package hk.ust.comp4651;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * You only need to complete TWO TODO methods:
 *   1) delEmptyFilesRecursive
 *   2) delBySuffixRecursive
 * Do NOT modify other parts.
 *
 * ---- Updates for robust local/HDFS runs ----
 * - Base path uses fs.getHomeDirectory() + "hw2" to work both with file:/// and hdfs:///
 * - Path safety check uses ensureUnder(...) with normalized paths and trailing-slash handling
 */
public class Part_3 {

    /** String helper retained for display/reference; not strictly used to build the actual base path. */
    
    private static String sandboxPrefix(String user) {
        return "/user/" + user + "/part3";
    }

    /** Returns a FileSystem instance. Do NOT modify. */
    private static FileSystem fs() throws IOException {
        return HDFSUtils.getFileSystem();
    }

    /** Recursively counts number of files and total bytes under root. Do NOT modify. */
    private static long[] countFilesAndBytes(FileSystem fs, Path root) throws IOException {
        long files = 0, bytes = 0;
        if (!fs.exists(root)) return new long[]{0, 0};
        for (FileStatus st : fs.listStatus(root)) {
            if (st.isDirectory()) {
                long[] sub = countFilesAndBytes(fs, st.getPath());
                files += sub[0];
                bytes += sub[1];
            } else {
                files += 1;
                bytes += st.getLen();
            }
        }
        return new long[]{files, bytes};
    }

    /**
     * Generates random test files directly in HDFS (including empty files and
     * files with different suffixes). Do NOT modify.
     */
    public static void generateTestFilesHDFS(Path baseDir, int nTxt, int nLog, int nTmp, int nEmpty) throws IOException {
        FileSystem fs = fs();
        fs.mkdirs(baseDir);

        Random rnd = new Random(42);

        // Create several non-empty .txt/.log/.tmp files
        for (int i = 0; i < nTxt; i++)
            writeRandomFile(fs, new Path(baseDir, "t_" + i + ".txt"), 1024 + rnd.nextInt(4096));
        for (int i = 0; i < nLog; i++)
            writeRandomFile(fs, new Path(baseDir, "l_" + i + ".log"), 512 + rnd.nextInt(2048));
        for (int i = 0; i < nTmp; i++)
            writeRandomFile(fs, new Path(baseDir, "x_" + i + ".tmp"), 128 + rnd.nextInt(1024));

        // Create several empty files
        for (int i = 0; i < nEmpty; i++) {
            Path p = new Path(baseDir, "empty_" + i);
            if (fs.exists(p)) fs.delete(p, false);
            fs.create(p, true).close();
        }
    }

    /** Writes a single random-content file of size 'sizeBytes' in HDFS. Do NOT modify. */
    private static void writeRandomFile(FileSystem fs, Path p, int sizeBytes) throws IOException {
        if (fs.exists(p)) fs.delete(p, false);
        try (FSDataOutputStream out = fs.create(p, true)) {
            byte[] buf = new byte[4096];
            Random r = new Random(p.toString().hashCode());
            int written = 0;
            while (written < sizeBytes) {
                int n = Math.min(buf.length, sizeBytes - written);
                r.nextBytes(buf);
                out.write(buf, 0, n);
                written += n;
            }
        }
    }

    /**
     * Path safety guard: ensure p is the root itself or under root.
     * Normalizes scheme/authority and adds a trailing slash to avoid false negatives.
     */
    private static void ensureUnder(FileSystem fs, Path root, Path p) throws IOException {
        Path qRoot = root.makeQualified(fs.getUri(), fs.getWorkingDirectory());
        Path qPath = p.makeQualified(fs.getUri(), fs.getWorkingDirectory());

        URI rUri = qRoot.toUri();
        URI pUri = qPath.toUri();

        String rSch = rUri.getScheme();
        String pSch = pUri.getScheme();
        String rAuth = rUri.getAuthority();
        String pAuth = pUri.getAuthority();

        // Scheme must match (e.g., both file or both hdfs)
        if ((rSch != null && !rSch.equalsIgnoreCase(pSch)) || (rSch == null && pSch != null)) {
            throw new IOException("Scheme mismatch: root=" + rSch + " vs path=" + pSch);
        }

        // Authority must match (for hdfs). For file:///, authority is usually null.
        if (rAuth == null ? pAuth != null : !rAuth.equalsIgnoreCase(pAuth)) {
            if (!"file".equalsIgnoreCase(rSch)) {
                throw new IOException("Authority mismatch: root=" + rAuth + " vs path=" + pAuth);
            }
        }

        String rootPath = rUri.getPath();
        String pPath    = pUri.getPath();

        if (rootPath == null) rootPath = "/";
        if (pPath == null)    pPath    = "/";

        String rootWithSlash = rootPath.endsWith("/") ? rootPath : (rootPath + "/");
        if (!(pPath.equals(rootPath) || pPath.startsWith(rootWithSlash))) {
            throw new IOException("Unsafe root: " + p + " (must be under " + rootWithSlash + ")");
        }
    }

    /** Recursively delete ALL empty files (length == 0) under 'root'. */
    public static void delEmptyFilesRecursive(Path root) throws IOException {
        FileSystem fs = fs();
        ensureUnder(fs, root, root);

        if(!fs.exists(root))
            return;

        FileStatus[] statuses = fs.listStatus(root);
        for (FileStatus status : statuses){
            Path currenPath = status.getPath();

            if(status.isDirectory()){
                delEmptyFilesRecursive(currenPath);
            }else{
                // check if file is empty 
                if(status.getLen() == 0){
                    fs.delete(currenPath, false);
                }
            }
        }
    }

    /**
     * Recursively delete ALL files whose names end with 'suffix' under 'root'.
     * Exact suffix match, case-sensitive. Example: ".tmp" matches "a.tmp" but NOT "a.tmp.bak".
     */
    public static void delBySuffixRecursive(Path root, String suffix) throws IOException {
        FileSystem fs = fs();
        ensureUnder(fs, root, root);

        if(!fs.exists(root))
            return;

        FileStatus[] statuses = fs.listStatus(root);
        for (FileStatus status : statuses){
            Path currenPath = status.getPath();

            if(status.isDirectory()){
                delBySuffixRecursive(currenPath, suffix);
            }else{
                // check if file name ends with the given suffix
                if(currenPath.getName().endsWith(suffix)){
                    fs.delete(currenPath, false);
                }
            }
        }
    }

    /**
     * Main flow (Do NOT modify):
     * 1) Recreate sandbox.
     * 2) Generate test data in HDFS (txt/log/tmp/empty).
     * 3) Print BEFORE summary; invoke your two deletion methods; print AFTER summary.
     */
    public static void main(String[] args) throws Exception {
        FileSystem fs = fs();
        String user = System.getProperty("user.name");

        // Use a robust, portable base under the user's Hadoop home directory
        Path base = new Path(fs.getHomeDirectory(), "part3");

        // Print both logical and actual sandbox for visibility
        System.out.println("Sandbox (logical): " + sandboxPrefix(user));
        System.out.println("Sandbox (actual) : " + base.makeQualified(fs.getUri(), fs.getWorkingDirectory()));

        // Repeatable runs: clean then create
        if (fs.exists(base)) fs.delete(base, true);
        fs.mkdirs(base);

        // 1) Generate data
        generateTestFilesHDFS(base, /*txt*/5, /*log*/3, /*tmp*/4, /*empty*/2);

        // 2) BEFORE summary
        long[] before = countFilesAndBytes(fs, base);
        System.out.printf("BEFORE files=%d bytes=%d%n", before[0], before[1]);

        // 3) Student implementations
        delEmptyFilesRecursive(base);
        delBySuffixRecursive(base, ".tmp");

        // 4) AFTER summary
        long[] after = countFilesAndBytes(fs, base);
        System.out.printf("AFTER  files=%d bytes=%d%n", after[0], after[1]);
    }
}
