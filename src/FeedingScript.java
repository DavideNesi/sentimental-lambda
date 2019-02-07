import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import java.io.*;
import java.util.Random;

public class FeedingScript {

    private static final String hdfsURI = "hdfs://localhost:9000";
    private static final String tweetPoolPath = "/dataset/dbTweet.csv";
    private static final String stormInputPath = "/storm/input/";
    private static final String stormProcessedPath = "/storm/processed/";
    private static final String hadoopInputPath = "/hadoop/input/";

    static int hadoopFileCounter = 0;
    static int stormFileCounter = 0;

    static final long hadoopMaxFileSize = 10000000;
    static final long stormMaxFileSize = 10000;

    static final int threadSleepTime = 100;

    public static BufferedWriter getNewHadoopWriter(FileSystem fs) throws IOException{
        Path path = new Path(hadoopInputPath + "input_file_" + hadoopFileCounter++ + ".txt");
        return new BufferedWriter( new OutputStreamWriter( fs.create(path), "UTF-8" ) );
    }

    public static BufferedWriter getNewStormWriter(FileSystem fs) throws IOException {
        Path path =  new Path(stormInputPath + "input_file_" + stormFileCounter++ + ".ignore");
        return new BufferedWriter( new OutputStreamWriter( fs.create(path), "UTF-8" ) );
    }

    public static void consolidateFiles(FileSystem fs) throws IOException{
        // Rename .ignore files in .txt
        FileStatus[] fileStatuses = fs.listStatus(new Path(stormInputPath), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.toString().contains("ignore");
            }
        });
        for(FileStatus file : fileStatuses){
            String path = file.getPath().toString();
            fs.rename(file.getPath(), new Path(path.replace("ignore", "txt")));
        }
        // Delete files in /storm/processed/
        fileStatuses = fs.listStatus(new Path(stormProcessedPath));
        for(FileStatus file : fileStatuses){
            fs.delete(file.getPath(), false);
        }
    }

    public static void main(String[] args) throws IOException {

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.defaultFS", hdfsURI);
        FileSystem fileSystem = FileSystem.get(hadoopConfig);
        Path tweetDBPath = new Path(tweetPoolPath);

        if(!fileSystem.exists(tweetDBPath))
            throw new IOException();

        BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(tweetDBPath)));

        BufferedWriter bw_hadoop = null;
        BufferedWriter bw_storm = null;

        Random rand = new Random();

        try {
            String line;
            line=br.readLine();
            long byteCounter_hadoop = 0;
            long byteCounter_storm = 0;

            bw_hadoop = getNewHadoopWriter(fileSystem);
            bw_storm = getNewStormWriter(fileSystem);

            while (line != null){

                if (byteCounter_hadoop + line.length() > hadoopMaxFileSize){ // max 10mb
                    bw_hadoop.close();
                    System.out.println("Wrote an hadoop file.");
                    byteCounter_hadoop = 0;
                    bw_hadoop = getNewHadoopWriter(fileSystem);
                }

                if (byteCounter_storm + line.length() > stormMaxFileSize){ // max 1mb
                    bw_storm.close();
                    System.out.println("Wrote a storm file.");
                    byteCounter_storm = 0;
                    consolidateFiles(fileSystem);
                    bw_storm = getNewStormWriter(fileSystem);
                }

                // Write a line on the hadoop file
                bw_hadoop.write(line);
                byteCounter_hadoop += line.length();

                if(rand.nextInt(5) + 1 == 5){ // And sometimes even on storm file
                    bw_storm.write(line);
                    byteCounter_storm += line.length();
                }

                System.out.print("Hadoop/Storm: ("+byteCounter_hadoop+","+hadoopMaxFileSize+")/("+byteCounter_storm+","+stormMaxFileSize+")\r");

                line = br.readLine();
                Thread.sleep(threadSleepTime);
            }
        } catch (InterruptedIOException ex){ System.out.println("Aborted from user."); }
          catch (Exception ex) { System.out.println(ex); }

        finally{
            br.close(); // Close db reader

            if (bw_hadoop != null)  // Then close the writers
                bw_hadoop.close();

            if(bw_storm != null)
                bw_storm.close();
        }

        fileSystem.close();
    }
}