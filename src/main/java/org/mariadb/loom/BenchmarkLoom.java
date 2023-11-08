package org.mariadb.loom;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.openjdk.jmh.annotations.*;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.sql.SQLException;
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

        @Param({"1000"})
        int numberOfTasks;

        @Param({"100"})
        int numberOfConnection;

        @Setup(Level.Trial)
        public void createConnections() throws InterruptedException, MalformedObjectNameException {

            HikariConfig config = new HikariConfig();
            config.setDriverClassName(
                    ("mariadb".equals(driver) ? "org.mariadb.jdbc.Driver" : "com.mysql.cj.jdbc.Driver"));
            config.setJdbcUrl(String.format("jdbc:%s://localhost:3306/testj", driver));
            config.setUsername("root");

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
        }

        @TearDown(Level.Trial)
        public void doTearDown() {
            pool.close();
        }
    }

    @Benchmark
    public void testVirtual(MyState state) throws InterruptedException {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            IntStream.range(0, state.numberOfTasks).forEach(i -> executor.submit(() -> {
                try (var conn = state.pool.getConnection()) {
                    conn.createStatement().executeQuery("SELECT 1");
                } catch (SQLException e) {
                    System.out.println("ERROR !");
                    throw new RuntimeException(e);
                }
            }));
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    @Benchmark
    public void testPlatform(MyState state) throws InterruptedException {
        try (var executor = Executors.newCachedThreadPool()) {
            IntStream.range(0, state.numberOfTasks).forEach(i -> executor.submit(() -> {
                try (var conn = state.pool.getConnection()) {
                    conn.createStatement().executeQuery("SELECT 1");
                } catch (SQLException e) {
                    System.out.println("ERROR !");
                    throw new RuntimeException(e);
                }
            }));
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

}