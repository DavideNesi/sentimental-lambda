package CassandraAdapter;

import com.datastax.driver.core.*;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;

public class CassandraDriver {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CassandraDriver.class);
    private Cluster cluster;
    private Session session;
    private PreparedStatement preparedStatement;

    public Session getSession()  {
       LOG.info("Starting getSession()");
        if (this.session == null && (this.cluster == null || this.cluster.isClosed())) {
            LOG.info("Cluster not started or closed");
        } else if (this.session.isClosed()) {
            LOG.info("session is closed. Creating a session");
            this.session = this.cluster.connect();
        }

        return this.session;
    }

    public void createConnection(String node)  {

        this.cluster = Cluster.builder().addContactPoint(node).build();

        Metadata metadata = cluster.getMetadata();

        System.out.printf("Connected to cluster: %s\n",metadata.getClusterName());

        for ( Host host : metadata.getAllHosts() ) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        this.session = cluster.connect();

    }

    public void closeConnection() {
        cluster.close();
    }


    public void addTweetCountEntry(String keyword, String sentiment, long count) {
        String query = "INSERT INTO SentimentAnalysis." + sentiment
                +"s (timestmp, keyword, count) VALUES(toTimeStamp(now()),?,?)";

        this.preparedStatement = this.session.prepare(query);
        Session session = this.getSession();

        try {
            if(keyword == null)
                keyword = "All dataset";

            session.execute(this.preparedStatement.bind(keyword, count) );
            //session.executeAsync(this.preparedStatement.bind(key));
        } catch (NoHostAvailableException e) {
            System.out.printf("No host in the %s cluster can be contacted to execute the query.\n",
                    session.getCluster());
            Session.State st = session.getState();
            for ( Host host : st.getConnectedHosts() ) {
                System.out.println("In flight queries::"+st.getInFlightQueries(host));
                System.out.println("open connections::"+st.getOpenConnections(host));
            }

        } catch (QueryExecutionException e) {
            System.out.println("An exception was thrown by Cassandra because it cannot " +
                    "successfully execute the query with the specified consistency level.");
        }  catch (IllegalStateException e) {
            System.out.println("The BoundStatement is not ready.");
        }

    }

    public void getTweetCountByKeyword(String keyword){

        if (keyword==null)
            keyword = "All dataset";

        this.preparedStatement = this.session.prepare(my_query);

        Session session = this.getSession();

        String sentiment[] = { "positives", "negatives", "neutrals"};
        long results[3];

        try {

            for(int i = 0; i < 3; i++){
                String my_query = "SELECT count FROM SentimentAnalysis." + sentiment[i]
                        + " WHERE keyword='" + keyword + "' ORDER BY timestmp DESC LIMIT 1";
                ResultSet rs  = session.execute(my_query);
                for(Row row : rs)
                    results[i] = row.getLong("count");
            }

            //session.executeAsync(this.preparedStatement.bind(key));
        } catch (NoHostAvailableException e) {
            System.out.printf("No host in the %s cluster can be contacted to execute the query.\n",
                    session.getCluster());
            Session.State st = session.getState();
            for ( Host host : st.getConnectedHosts() ) {
                System.out.println("In flight queries::"+st.getInFlightQueries(host));
                System.out.println("open connections::"+st.getOpenConnections(host));
            }

        } catch (QueryExecutionException e) {
            System.out.println("An exception was thrown by Cassandra because it cannot " +
                    "successfully execute the query with the specified consistency level.");
        }  catch (IllegalStateException e) {
            System.out.println("The BoundStatement is not ready.");
        }

    }

}