
package CassandraAdapter;


public class CassandraTester {

    public static void main(String[] args) throws InterruptedException {

        // This tester use SentimentAnalysis.queries (keyword, timestmp, pos_count, neg_count)

        String host = "127.0.0.1";

        CassandraDriver client = new CassandraDriver();

        //Create the connection
        client.createConnection(host);

        System.out.println("starting writes");

        //Add test value
        client.addTweetCountEntry(null, "positive", 29);

        long c[] = {25, 37 ,20};
        client.addTweetCountStorm(null, c);

        client.getTweetCountByKeyword(null);

        //Close the connection
        client.closeConnection();

        System.out.println("Write Complete");
    }
}