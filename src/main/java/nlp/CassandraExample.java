package nlp;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class CassandraExample implements AutoCloseable {
    private static final String PROXY_HOST = "proxy.internal.company"; // Change this to your proxy server
    private static final int PROXY_PORT = 1080; // Typical SOCKS5 proxy port
    private static final String CASSANDRA_HOST = "localhost"; // Internal Cassandra host
    private static final int CASSANDRA_PORT = 9042;
    private final CqlSession session;

    public CassandraExample() {
        this.session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(CASSANDRA_HOST, CASSANDRA_PORT))
                .withLocalDatacenter("datacenter1") // Adjust based on your setup
                .withAuthCredentials("cassandra", "cassandra") // Change credentials if needed
                .build();
    }

    public void createSchema() {
        final var createKeyspace = "CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};";

        final var createTable = """
                    CREATE TABLE IF NOT EXISTS mykeyspace.inverted_index (
                            tkn text,
                            eid text,
                            PRIMARY KEY (tkn, eid)
                    ) WITH CLUSTERING ORDER BY (eid ASC);
                """;
        session.execute(createKeyspace);
        session.execute(createTable);
    }

    public void insert(String eid, String[] tkns) {
        final var insertInvertedIndex = "INSERT INTO mykeyspace.inverted_index (tkn, eid) VALUES (?, ?);";
        final var preparedInsertOfInvertedIndex = session.prepare(insertInvertedIndex);
        final var maxBatchSize = 5000;

        // Process tokens in batches
        for (var i = 0; i < tkns.length; i += maxBatchSize) {
            final var batch = BatchStatement.builder(BatchType.UNLOGGED);
            final var end = Math.min(i + maxBatchSize, tkns.length);
            for (int j = i; j < end; j++) {
                batch.addStatement(preparedInsertOfInvertedIndex.bind(tkns[j], eid));
            }
            session.execute(batch.build());
        }

        System.out.println("Inserted");
    }

    public void insertBulk(String eid, String[] tkns) {
        final var insertQuery = "INSERT INTO mykeyspace.inverted_index (tkn, eid) VALUES (?, ?);";
        final var preparedStatement = session.prepare(insertQuery);

        // List to hold all asynchronous insert operations
        final var futures = new ArrayList<CompletableFuture<AsyncResultSet>>();

        // Issue asynchronous insert for each token
        for (final var tkn : tkns) {
            CompletableFuture<AsyncResultSet> future =
                    session.executeAsync(preparedStatement.bind(tkn, eid))
                            .toCompletableFuture();
            futures.add(future);
        }

        // Wait for all insert operations to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        System.out.println("Inserted");
    }


    public void query() {
        String cqlQuery = "SELECT tkn, count(*) FROM mykeyspace.inverted_index GROUP BY tkn";

        try {
            ResultSet resultSet = session.execute(cqlQuery);
            for (final var row : resultSet) {
                final var token = row.getString("tkn");
                final var count = row.getLong("count"); // "count" is the alias for count(*)
                System.out.println("tkn: " + token + ", count: " + count);
            }
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            // Consider more robust error handling like logging or throwing a custom exception
        }
    }

    public void query2(String tkn) {
        String query = "SELECT tkn, count(*) FROM mykeyspace.inverted_index WHERE tkn=?";
        final var preparedStatement = session.prepare(query);

        try {
            ResultSet resultSet = session.execute(preparedStatement.bind(tkn));
            for (final var row : resultSet) {
                final var token = row.getString("tkn");
                final var count = row.getLong("count"); // "count" is the alias for count(*)
                System.out.println("tkn: " + token + ", count: " + count);
            }
        } catch (Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            // Consider more robust error handling like logging or throwing a custom exception
        }
    }

    @Override
    public void close() throws Exception {
        session.close();
    }
}
