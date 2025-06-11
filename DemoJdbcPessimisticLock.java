import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Version: 0.1.0
 */
public class DemoJdbcPessimisticLock {

    public static String[] connectionTags = new String[] { "Connection A", "Connection B" };
    public static List<Connection> connections = new ArrayList<Connection>();
    public static BigDecimal id = null;

    public static void printResultSetStringString(String stmtText, Connection connection) {
        int count = 0;
        System.out.println("\n/* Executing query: " + stmtText + "; */");
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(stmtText);
            System.out.println("\tRow#, " + resultSet.getMetaData().getColumnName(1) + ", "
                    + resultSet.getMetaData().getColumnName(2));
            while (resultSet.next()) {
                System.out.println("\t" + (++count) + ") " + resultSet.getString(1) + ", " + resultSet.getString(2));
            }
            resultSet.close();
            statement.close();
            System.out.println();
        } catch (SQLException e) {
            System.out.println("Error: " + e);
        }
    }

    static class RowUpdater implements Runnable {

        private int connectionNo;
        private BigDecimal rowid;
        private int wait;
        private int waitBefore1stCommit;

        public RowUpdater(int connectionNo, BigDecimal rowid, int wait, int waitBefore1stCommit) {
            this.connectionNo = connectionNo;
            this.rowid = rowid;
            this.wait = wait;
            this.waitBefore1stCommit = waitBefore1stCommit;
        }

        @Override
        public void run() {
            System.out.println(connectionTags[this.connectionNo] + " session started");
            Connection c = connections.get(this.connectionNo);
            try {
                Statement s = c.createStatement();
                try {
                    Thread.sleep(wait);
                } catch (InterruptedException e2) {
                    e2.printStackTrace();
                }
                System.out.println(connectionTags[this.connectionNo] + " session: " + "BEGIN PESSIMISTIC");
                s.executeUpdate("BEGIN PESSIMISTIC");
                System.out.println(connectionTags[this.connectionNo] + " session: "
                        + "UPDATE test_tx_pessimistic SET name = '" + connectionTags[this.connectionNo]
                        + "' WHERE id = " + rowid);
                s.executeUpdate("UPDATE test_tx_pessimistic SET name = '" + connectionTags[this.connectionNo]
                        + "' WHERE id = " + rowid);
                try {
                    Thread.sleep(this.waitBefore1stCommit);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(connectionTags[this.connectionNo] + " session: " + "Commit");
                c.commit();

            } catch (SQLException e) {
                System.out.println(connectionTags[this.connectionNo] + " ErrorCode: " + e.getErrorCode());
                System.out.println(connectionTags[this.connectionNo] + " SQLState: " + e.getSQLState());
                System.out.println(connectionTags[this.connectionNo] + " Error: " + e);
            } finally {
                System.out.println(connectionTags[this.connectionNo] + " session: " + "Checking result");
                printResultSetStringString("select id, name from test_tx_pessimistic", c);
            }
        }
    }

    public static void main(String[] args) {

        String tidbHost = System.getenv().getOrDefault("TIDB_HOST", "localhost");
        int tidbPort = Integer.parseInt(System.getenv().getOrDefault("TIDB_PORT", "4000"));
        String tidbUser = System.getenv().getOrDefault("TIDB_USER", "root");
        String tidbPassword = System.getenv().getOrDefault("TIDB_PASSWORD", "");
        String tidbDatabase = System.getenv().getOrDefault("TIDB_DATABASE", "test");
        String connection_string = "jdbc:mysql://" + tidbHost + ":" + tidbPort + "/test?user=" + 
                tidbUser + "&password=" + tidbPassword + "&sslMode=VERIFY_IDENTITY&enabledTLSProtocols=TLSv1.2,TLSv1.3";
        System.out.println("Connection established.");

        try {
            for (int i = 0; i < 2; i++) {
                connections.add(DriverManager.getConnection(connection_string));
            }
            System.out.println("Connection established.");
            
            Statement s = connections.get(0).createStatement();
            s.executeUpdate("DROP TABLE IF EXISTS test_tx_pessimistic");
            s.executeUpdate("CREATE TABLE test_tx_pessimistic (id BIGINT PRIMARY KEY AUTO_RANDOM, name char(20))");
            s.executeUpdate("INSERT INTO test_tx_pessimistic (name) VALUES ('INIT') ", Statement.RETURN_GENERATED_KEYS);
            ResultSet rs = s.getGeneratedKeys();
            rs.first();
            id = rs.getBigDecimal(1);
            rs.close();
            s.close();

            for (Connection c : connections) {
                c.setAutoCommit(false);
            }

            new Thread(new DemoJdbcPessimisticLock.RowUpdater(0, id, 1, 6000)).start();
            new Thread(new DemoJdbcPessimisticLock.RowUpdater(1, id, 1000, 2000)).start();

        } catch (SQLException e) {
            System.out.println("Main Block ErrorCode: " + e.getErrorCode());
            System.out.println("Main Block SQLState: " + e.getSQLState());
            System.out.println("Main Block Error: " + e);
        }
    }
}
