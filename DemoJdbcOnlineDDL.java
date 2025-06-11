import java.sql.*;
// import java.util.concurrent.TimeUnit;

public class DemoJdbcOnlineDDL {
    
    private static Connection getConnection() throws SQLException {
        String tidbHost = System.getenv().getOrDefault("TIDB_HOST", "localhost");
        int tidbPort = Integer.parseInt(System.getenv().getOrDefault("TIDB_PORT", "4000"));
        String tidbUser = System.getenv().getOrDefault("TIDB_USER", "root");
        String tidbPassword = System.getenv().getOrDefault("TIDB_PASSWORD", "");
        String tidbDatabase = System.getenv().getOrDefault("TIDB_DATABASE", "test");
        
        // TiDB Cloud requires SSL connections
        String connectionString = "jdbc:mysql://" + tidbHost + ":" + tidbPort + "/" + tidbDatabase + 
                                 "?user=" + tidbUser + "&password=" + tidbPassword + 
                                 "&sslMode=VERIFY_IDENTITY&enabledTLSProtocols=TLSv1.2,TLSv1.3";
        
        System.out.println("Connecting to: " + tidbHost + ":" + tidbPort + " with database: " + tidbDatabase);
        
        try {
            Connection conn = DriverManager.getConnection(connectionString);
            System.out.println("Connection successful!");
            
            // Test the connection with a simple query
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {
                if (rs.next()) {
                    System.out.println("Database connection test passed!");
                }
            }
            
            return conn;
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            throw e;
        }
    }
    
    private static void setupTable(Connection connection, String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Disable TiDB metadata lock for online DDL operations
            try {
                stmt.execute("SET GLOBAL tidb_enable_metadata_lock = OFF");
                System.out.println("Disabled TiDB metadata lock for online DDL operations");
                
                // Show and print the variable status
                try (ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'tidb_enable_metadata_lock'")) {
                    if (rs.next()) {
                        System.out.println("Current setting: " + rs.getString(1) + " = " + rs.getString(2));
                    } else {
                        System.out.println("Could not retrieve variable status");
                    }
                }
            } catch (SQLException err) {
                System.out.println("Warning: Could not set tidb_enable_metadata_lock: " + err.getMessage());
                System.out.println("Continuing with default settings...");
            }
            
            // Drop table if it exists
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            
            // Create test table
            stmt.execute(String.format("""
                CREATE TABLE %s (
                    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
                    k INT NOT NULL,
                    c CHAR(120) NOT NULL,
                    pad CHAR(60) NOT NULL,
                    INDEX k_idx (k)
                )
                """, tableName));
            
            System.out.println("Created table " + tableName);
        }
    }
    
    private static boolean insertRecord(Connection connection, PreparedStatement pstmt, int roundNum) {
        System.out.println("Inserting record " + roundNum);
        
        try {
            // Attempt to execute the prepared statement
            pstmt.setInt(1, roundNum);
            pstmt.executeUpdate();
            
            // Simulate some work
            Thread.sleep(1000);
            connection.commit();
            return true;
            
        } catch (SQLException err) {
            // Check if it's error code 8028 (schema mutation)
            if (err.getErrorCode() == 8028) {
                System.out.println("Schema mutation encountered, retrying...");
                int retryAttempts = 0;
                int maxRetries = 5;
                int backoffTime = 1; // Start with 1 second backoff
                
                while (retryAttempts < maxRetries) {
                    try {
                        Thread.sleep(backoffTime * 1000);
                        pstmt.setInt(1, roundNum);
                        pstmt.executeUpdate();
                        Thread.sleep(1000);
                        connection.commit();
                        return true;
                        
                    } catch (SQLException retryErr) {
                        if (retryErr.getErrorCode() == 8028) {
                            retryAttempts++;
                            backoffTime *= 2; // Exponential backoff
                            System.out.println("Retry " + retryAttempts + " failed: " + retryErr.getMessage() + 
                                             ". Retrying in " + backoffTime + " seconds...");
                        } else {
                            System.out.println("Retry failed with different error: " + retryErr.getMessage());
                            return false;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.out.println("Thread interrupted during retry backoff");
                        return false;
                    }
                }
                System.out.println("Max retries reached. Moving to next record.");
                return false;
            } else {
                // Handle other MySQL errors
                System.out.println("Error: " + err.getMessage());
                if (err.getErrorCode() == 1045) { // Access denied
                    System.out.println("Check your username and password");
                } else if (err.getErrorCode() == 1049) { // Bad database
                    System.out.println("Database does not exist");
                } else {
                    System.out.println("Error code: " + err.getErrorCode());
                }
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Thread interrupted during sleep");
            return false;
        }
    }
    
    private static void runInsertJob(String tableName) {
        try (Connection connection = getConnection()) {
            connection.setAutoCommit(false);
            
            DatabaseMetaData metaData = connection.getMetaData();
            System.out.println("Connected to TiDB: " + metaData.getUserName() + 
                             "@" + metaData.getURL());
            
            // Setup the test table
            setupTable(connection, tableName);
            connection.commit();
            
            // SQL for inserting data
            String insertSql = "INSERT INTO " + tableName + " (k, c, pad) VALUES (?, 'A', 'B')";
            
            try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
                // Run continuous insert loop
                int roundNum = 0;
                while (true) {
                    boolean success = insertRecord(connection, pstmt, roundNum);
                    if (success) {
                        roundNum++;
                    } else {
                        // Pause briefly before retrying on failure
                        Thread.sleep(2000);
                    }
                }
            }
            
        } catch (SQLException err) {
            System.out.println("Connection error: " + err.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("\nInsert job stopped by user");
        }
    }
    
    private static void runAlterTable(String tableName) {
        try (Connection connection = getConnection()) {
            connection.setAutoCommit(true);
            
            DatabaseMetaData metaData = connection.getMetaData();
            System.out.println("Connected to TiDB: " + metaData.getUserName() + 
                             "@" + metaData.getURL());
            
            try (Statement stmt = connection.createStatement()) {
                // Check if table exists
                try (ResultSet rs = stmt.executeQuery("SELECT 1 FROM " + tableName + " LIMIT 1")) {
                    System.out.println("Table " + tableName + " exists, proceeding with ALTER TABLE");
                } catch (SQLException err) {
                    System.out.println("Table " + tableName + " does not exist or cannot be accessed. Error: " + err.getMessage());
                    System.out.println("Make sure to run the insert job first to create the table.");
                    return;
                }
                
                // Execute ALTER TABLE
                String alterStmt = "ALTER TABLE " + tableName + " ADD COLUMN ed VARCHAR(10) DEFAULT 'N/A'";
                System.out.println("Executing: " + alterStmt);
                
                long startTime = System.currentTimeMillis();
                
                // Execute the DDL statement
                stmt.execute(alterStmt);
                
                long endTime = System.currentTimeMillis();
                
                System.out.println("ALTER TABLE completed successfully in " + 
                                 (endTime - startTime) / 1000.0 + " seconds");
                
                // Verify the new column exists
                try (ResultSet rs = stmt.executeQuery("DESCRIBE " + tableName)) {
                    System.out.println("\nUpdated table structure:");
                    while (rs.next()) {
                        System.out.println("  " + rs.getString(1) + ": " + rs.getString(2));
                    }
                }
            }
            
        } catch (SQLException err) {
            System.out.println("Error executing ALTER TABLE: " + err.getMessage());
            System.out.println("Error code: " + err.getErrorCode());
            System.exit(1);
        }
    }
    
    private static void printUsage() {
        System.out.println("Usage: java DemoJdbcOnlineDDL [--insert|--alter] [table_name]");
        System.out.println("  --insert    Run the insert job");
        System.out.println("  --alter     Execute the ALTER TABLE command");
        System.out.println("  table_name  Name of the table to use (default: online_ddl_test)");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java DemoJdbcOnlineDDL --insert");
        System.out.println("  java DemoJdbcOnlineDDL --alter online_ddl_test");
    }
    
    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }
        
        String operation = null;
        String tableName = "online_ddl_test";
        
        // Simple argument parsing
        for (int i = 0; i < args.length; i++) {
            if ("--insert".equals(args[i])) {
                operation = "insert";
            } else if ("--alter".equals(args[i])) {
                operation = "alter";
            } else if (!args[i].startsWith("--")) {
                // Assume it's the table name
                tableName = args[i];
            }
        }
        
        if (operation == null) {
            System.out.println("Error: Must specify either --insert or --alter");
            printUsage();
            System.exit(1);
        }
        
        if ("insert".equals(operation)) {
            System.out.println("Starting insert job on table " + tableName);
            System.out.println("You can now run ALTER TABLE in another terminal with:");
            System.out.println("java DemoJdbcOnlineDDL --alter " + tableName);
            runInsertJob(tableName);
        } else if ("alter".equals(operation)) {
            System.out.println("Executing ALTER TABLE on " + tableName);
            runAlterTable(tableName);
        }
    }
}