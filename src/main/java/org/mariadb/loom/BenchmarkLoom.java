package org.mariadb.loom;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 5)
@Threads(value = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BenchmarkLoom {

    @State(Scope.Benchmark)
    public static class MyState {

        // connections
        protected HikariDataSource pool;

        @Param({"mariadb", "mysql"})
        String driver;

        @Param({"100"})
        int numberOfTasks;

        @Param({"16"})
        int numberOfConnection;

        @Setup(Level.Trial)
        public void createConnections() throws Exception {

            HikariConfig config = new HikariConfig();
            config.setDriverClassName(
                    ("mariadb".equals(driver) ? "org.mariadb.jdbc.Driver" : "com.mysql.cj.jdbc.Driver"));
            config.setJdbcUrl(String.format("jdbc:%s://104.248.141.106:3306/testj", driver));
            config.setUsername("diego");

            // in order to compare the same thing with mysql and mariadb driver,
            config.addDataSourceProperty("sslMode", "DISABLED");
            config.addDataSourceProperty("serverTimezone", "UTC");

            config.setMaximumPoolSize(numberOfConnection);
            config.setPoolName("foo");
            config.setRegisterMbeans(true);
            pool = new HikariDataSource(config);
            for (int i = 0; i < 100; i++) {
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                ObjectName poolName = new ObjectName("com.zaxxer.hikari:type=Pool (foo)");
                HikariPoolMXBean poolProxy = JMX.newMXBeanProxy(mBeanServer, poolName, HikariPoolMXBean.class);
                System.out.println("Total Connections: " + poolProxy.getTotalConnections() + " after " + (i * 0.1) + "s)");
                if (poolProxy.getTotalConnections() == numberOfConnection) return;
                // to ensure pool create all connections
                Thread.sleep(100);
            }
            try (Connection conn = pool.getConnection()) {
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("DROP TABLE IF EXISTS test100");
                StringBuilder sb = new StringBuilder("CREATE TABLE test100 (i1 int");
                StringBuilder sb2 = new StringBuilder("INSERT INTO test100 value (1");
                for (int i = 2; i <= 100; i++) {
                    sb.append(",i").append(i).append(" int");
                    sb2.append(",").append(i);
                }
                sb.append(")");
                sb2.append(")");
                stmt.executeUpdate(sb.toString());
                stmt.executeUpdate(sb2.toString());
            }
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
            pool.close();
        }
    }

    @Benchmark
    public void Select1Virtual(MyState state, Blackhole blackHole) throws InterruptedException {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executeSelect1(state, executor, blackHole);
        }
    }

    @Benchmark
    public void Select1Platform(MyState state, Blackhole blackHole) throws InterruptedException {
        try (var executor = Executors.newCachedThreadPool()) {
            executeSelect1(state, executor, blackHole);
        }
    }

    private void executeSelect1 (MyState state, ExecutorService executor, Blackhole blackHole) throws InterruptedException {
        IntStream.range(0, state.numberOfTasks).forEach(i -> executor.submit(() -> {
            try (var conn = state.pool.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select 1")) {
                        rs.next();
                        blackHole.consume(rs.getInt(1));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }));
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Benchmark
    public void Select100ColsVirtual(MyState state, Blackhole blackHole) throws InterruptedException {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executeSelect100Cols(state, executor, blackHole);
        }
    }

    @Benchmark
    public void Select100ColsPlatform(MyState state, Blackhole blackHole) throws InterruptedException {
        try (var executor = Executors.newCachedThreadPool()) {
            executeSelect100Cols(state, executor, blackHole);
        }
    }
    private void executeSelect100Cols(MyState state, ExecutorService executor, Blackhole blackHole) throws InterruptedException {
        IntStream.range(0, state.numberOfTasks).forEach(i -> executor.submit(() -> {
            try (var conn = state.pool.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select * FROM test100")) {
                        rs.next();
                        for (int ii = 1; ii <= 100; ii++) blackHole.consume(rs.getInt(ii));
                    }
                }
            } catch (SQLException e) {
                System.out.println(e);
            }
        }));
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Benchmark
    public void Select1000RowsVirtual(MyState state, Blackhole blackHole) throws InterruptedException {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executeSelect1000Rows(state, executor, blackHole);
        }
    }

    @Benchmark
    public void Select1000RowsPlatform(MyState state, Blackhole blackHole) throws InterruptedException {
        try (var executor = Executors.newCachedThreadPool()) {
            executeSelect1000Rows(state, executor, blackHole);
        }
    }
    private void executeSelect1000Rows(MyState state, ExecutorService executor, Blackhole blackHole) throws InterruptedException {
        IntStream.range(0, state.numberOfTasks).forEach(i -> executor.submit(() -> {
            try (var conn = state.pool.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("select seq from seq_1_to_1000")) {
                        while (rs.next()) {
                            rs.next();
                            blackHole.consume(rs.getInt(1));
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }));
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

}