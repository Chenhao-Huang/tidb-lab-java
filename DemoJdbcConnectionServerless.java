import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DemoJdbcConnectionServerless {
    public static void main(String[] args) {
        Connection connection = null;
        try {
            String tidbHost = System.getenv().getOrDefault("TIDB_HOST", "localhost");
            int tidbPort = Integer.parseInt(System.getenv().getOrDefault("TIDB_PORT", "4000"));
            String tidbUser = System.getenv().getOrDefault("TIDB_USER", "root");
            String tidbPassword = System.getenv().getOrDefault("TIDB_PASSWORD", "");
            String tidbDatabase = System.getenv().getOrDefault("TIDB_DATABASE", "test");
            String connection_string = "jdbc:mysql://" + tidbHost + ":" + tidbPort + "/test?user=" + 
                    tidbUser + "&password=" + tidbPassword + "&sslMode=VERIFY_IDENTITY&enabledTLSProtocols=TLSv1.2,TLSv1.3";

            connection = DriverManager.getConnection(connection_string);
            System.out.println("Connection established.");
        } catch (Exception e) {
            System.out.println("Error: " + e);
        } finally {
            if (connection != null) {
                try {
                    // Release the resources in cascade
                    connection.close();
                    System.out.println("Connection closed.");
                } catch (SQLException e) {
                    System.out.println("Error disconnecting: " + e.toString());
                }
            } else {
                System.out.println("Already disconnected.");
            }
        }
    }
}