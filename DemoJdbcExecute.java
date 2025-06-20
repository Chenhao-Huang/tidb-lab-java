import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;

/**
 * Execute multi-statement?
 *   Error: java.sql.SQLException: client has multi-statement capability disabled.
 *   Run SET GLOBAL tidb_multi_statement_mode='ON' after you understand the security risk.
 */

public class DemoJdbcExecute {

    public static void printResultSetStringString(String stmtText, Connection connection) {
        int count = 0;
        System.out.println("\n/* Executing: "+stmtText+"; */");
        try {
            Statement statement = connection.createStatement();
            Boolean isResultSet = statement.execute(stmtText);
            if (isResultSet){
                ResultSet resultSet = statement.getResultSet();
                System.out.println("\tRow#,  "+resultSet.getMetaData().getColumnName(1)+", "+resultSet.getMetaData().getColumnName(2));
                while (resultSet.next()) {
                    System.out.println("\t"+(++count) + ") " + resultSet.getString(1)+", "+resultSet.getString(2));
                }    
                resultSet.close();
            }
            statement.close();
        } catch (Exception e) {
            System.out.println("Error: "+e.toString());
        }
    }

    public static void main(String[] args) {
        Connection connection = null;
        try {
            String tidbHost = System.getenv().getOrDefault("TIDB_HOST", "localhost");
            int tidbPort = Integer.parseInt(System.getenv().getOrDefault("TIDB_PORT", "4000"));
            String tidbUser = System.getenv().getOrDefault("TIDB_USER", "root");
            String tidbPassword = System.getenv().getOrDefault("TIDB_PASSWORD", "");
            String tidbDatabase = System.getenv().getOrDefault("TIDB_DATABASE", "test");

            String connectionUrl = "jdbc:mysql://" + tidbHost + ":" + tidbPort + "/" + tidbDatabase + 
                    "?sslMode=VERIFY_IDENTITY&enabledTLSProtocols=TLSv1.2,TLSv1.3";
            connection = DriverManager.getConnection(connectionUrl, tidbUser, tidbPassword);

            System.out.println("Connection established.");
            // Turn on multi-statement
            printResultSetStringString("SET tidb_multi_statement_mode='ON'", connection);
            // Show autocommit
            printResultSetStringString("show variables like 'autocommit'", connection);
            // Create table
            connection.createStatement().executeUpdate("DROP TABLE IF EXISTS t1");
            connection.createStatement().executeUpdate("CREATE TABLE t1 (id int PRIMARY KEY, name char(4))");
            // Describe table
            printResultSetStringString("DESCRIBE test.t1", connection);
            // Explain SQL
            printResultSetStringString("EXPLAIN SELECT * FROM test.t1", connection);
            // Select
            printResultSetStringString("SELECT * FROM test.t1", connection);
            // Try DML
            printResultSetStringString("INSERT INTO test.t1 VALUES (100, 'WXYZ'); INSERT INTO test.t1 VALUES (200, 'ABCD')", connection);
            // Select again
            printResultSetStringString("SELECT * FROM test.t1", connection);
            // Finishing.
        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                    System.out.println("Connection closed.");
                } catch (Exception e) {
                    System.out.println("Error disconnecting: " + e.toString());
                }
            }
            else{
                System.out.println("Already disconnected.");
            }
        }
    }
}