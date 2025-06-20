import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
// import java.util.concurrent.atomic.AtomicInteger;

public class DemoJdbcAutoRandom {
    
    private static Connection getConnection() throws SQLException {
        Connection connection = null;
        
        String tidbHost = System.getenv().getOrDefault("TIDB_HOST", "localhost");
        int tidbPort = Integer.parseInt(System.getenv().getOrDefault("TIDB_PORT", "4000"));
        String tidbUser = System.getenv().getOrDefault("TIDB_USER", "root");
        String tidbPassword = System.getenv().getOrDefault("TIDB_PASSWORD", "");
        String tidbDatabase = System.getenv().getOrDefault("TIDB_DATABASE", "test");
        
        String connectionUrl = "jdbc:mysql://" + tidbHost + ":" + tidbPort + "/" + tidbDatabase + 
                    "?sslMode=VERIFY_IDENTITY&enabledTLSProtocols=TLSv1.2,TLSv1.3";
        
        connection = DriverManager.getConnection(connectionUrl, tidbUser, tidbPassword);
        
        return connection;
    }
    
    private static void setupTables(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Drop tables if they exist
            stmt.execute("DROP TABLE IF EXISTS auto_random_demo");
            stmt.execute("DROP TABLE IF EXISTS auto_increment_demo");
            
            // Create a table with AUTO_RANDOM
            stmt.execute("""
                CREATE TABLE auto_random_demo (
                    id BIGINT PRIMARY KEY AUTO_RANDOM,
                    name VARCHAR(255)
                )
                """);
            
            // Create a similar table with AUTO_INCREMENT for comparison
            stmt.execute("""
                CREATE TABLE auto_increment_demo (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(255)
                )
                """);
            
            System.out.println("Tables created successfully.");
        }
    }
    
    private static void showTableDefinition(Connection connection, String tableName) throws SQLException {
        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName)) {
            
            System.out.println("\nTable definition for " + tableName + ":");
            if (rs.next()) {
                System.out.println(rs.getString(2)); // The second column contains the CREATE TABLE statement
            }
        }
    }
    
    private static class ConcurrentInsertWorker implements Runnable {
        private final String tableName;
        private final List<String> values;
        private final String threadId;
        
        public ConcurrentInsertWorker(String tableName, List<String> values, String threadId) {
            this.tableName = tableName;
            this.values = values;
            this.threadId = threadId;
        }
        
        @Override
        public void run() {
            try (Connection connection = getConnection()) {
                String insertStmt = "INSERT INTO " + tableName + " (name) VALUES (?)";
                try (PreparedStatement pstmt = connection.prepareStatement(insertStmt)) {
                    for (int i = 0; i < values.size(); i++) {
                        pstmt.setString(1, values.get(i));
                        pstmt.executeUpdate();
                        
                        if (i % 5 == 0) { // Print progress every 5 inserts
                            System.out.println("Thread " + threadId + ": Inserted '" + values.get(i) + "' into " + tableName);
                        }
                    }
                }
                
                System.out.println("Thread " + threadId + ": Completed all inserts for " + tableName);
            } catch (SQLException e) {
                System.out.println("Thread " + threadId + ": Error inserting into " + tableName + ": " + e.getMessage());
            }
        }
    }
    
    private static void insertDataConcurrent(int count, int numThreads) throws InterruptedException {
        // Generate values for each table
        List<String> autoRandomValues = new ArrayList<>();
        List<String> autoIncrementValues = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            autoRandomValues.add("Auto-random value " + i);
            autoIncrementValues.add("Auto-increment value " + i);
        }
        
        // Divide work among threads
        int itemsPerThread = count / numThreads;
        List<Thread> threads = new ArrayList<>();
        
        System.out.println("\nStarting " + numThreads + " threads to insert " + count + " rows into each table...");
        
        // Create and start threads for AUTO_RANDOM table
        for (int i = 0; i < numThreads; i++) {
            int startIdx = i * itemsPerThread;
            int endIdx = (i < numThreads - 1) ? startIdx + itemsPerThread : count;
            List<String> threadValues = autoRandomValues.subList(startIdx, endIdx);
            
            Thread thread = new Thread(new ConcurrentInsertWorker("auto_random_demo", threadValues, "AR-" + (i + 1)));
            threads.add(thread);
            thread.start();
        }
        
        // Create and start threads for AUTO_INCREMENT table
        for (int i = 0; i < numThreads; i++) {
            int startIdx = i * itemsPerThread;
            int endIdx = (i < numThreads - 1) ? startIdx + itemsPerThread : count;
            List<String> threadValues = autoIncrementValues.subList(startIdx, endIdx);
            
            Thread thread = new Thread(new ConcurrentInsertWorker("auto_increment_demo", threadValues, "AI-" + (i + 1)));
            threads.add(thread);
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }
        
        System.out.println("Concurrent insert completed: " + count + " rows inserted into each table using " + numThreads + " threads");
    }
    
    private static void compareIds(Connection connection) throws SQLException {
        // Get AUTO_RANDOM IDs
        List<Long> autoRandomIds = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM auto_random_demo ORDER BY id")) {
            while (rs.next()) {
                autoRandomIds.add(rs.getLong("id"));
            }
        }
        
        // Get AUTO_INCREMENT IDs
        List<Long> autoIncrementIds = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM auto_increment_demo ORDER BY id")) {
            while (rs.next()) {
                autoIncrementIds.add(rs.getLong("id"));
            }
        }
        
        System.out.println("\n=== ID Comparison: AUTO_RANDOM vs AUTO_INCREMENT ===");
        System.out.println("\nAUTO_RANDOM IDs (distributed to avoid hotspots):");
        for (Long id : autoRandomIds) {
            System.out.println(id);
        }
        
        System.out.println("\nAUTO_INCREMENT IDs (sequential, can cause hotspots):");
        for (Long id : autoIncrementIds) {
            System.out.println(id);
        }
    }
    
    private static void cleanup(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS auto_random_demo");
            stmt.execute("DROP TABLE IF EXISTS auto_increment_demo");
            System.out.println("\nCleanup complete. Test tables dropped.");
        }
    }
    
    public static void main(String[] args) {
        try (Connection connection = getConnection()) {
            System.out.println("Connected to TiDB: " + connection.getMetaData().getUserName() + 
                             "@" + connection.getMetaData().getURL());
            
            // Set autocommit to false for transactional operations
            connection.setAutoCommit(false);
            
            // Create tables
            setupTables(connection);
            connection.commit();
            
            // Show table definitions
            showTableDefinition(connection, "auto_increment_demo");
            showTableDefinition(connection, "auto_random_demo");
            
            // Insert data using multiple concurrent threads
            // Adjust these parameters to control the concurrency level and total insert count
            int numThreads = 5;  // Number of concurrent threads per table
            int totalInserts = 10;  // Total number of rows to insert into each table
            
            insertDataConcurrent(totalInserts, numThreads);
            
            // Compare the IDs
            compareIds(connection);
            
            // Cleanup
            cleanup(connection);
            connection.commit();
            
        } catch (SQLException | InterruptedException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}