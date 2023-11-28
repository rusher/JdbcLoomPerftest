package org.mariadb.loom;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbResult;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1)
@Threads(value = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class BenchmarkLoom {

    @Benchmark
    public void Select1Platform(MyState state, Blackhole blackHole) throws InterruptedException {
        try (var executor = Executors.newCachedThreadPool()) {
            executeSelect1(state, executor, blackHole);
        }
    }

    @Benchmark
    public void Select1Virtual(MyState state, Blackhole blackHole) throws InterruptedException {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executeSelect1(state, executor, blackHole);
        }
    }

    private void executeSelect1(MyState state, ExecutorService executor, Blackhole blackHole) throws InterruptedException {
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
    public void Select100ColsR2DBC(MyState state, Blackhole blackHole) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(state.numberOfTasks);
        IntStream.range(0, state.numberOfTasks).forEach(i -> {
            state.poolR2Dbc.create()
                    .flatMap(connection -> ((Flux<MariadbResult>) connection.createStatement("select * FROM test100").execute())
                            .flatMap(it -> it.map((row, metadata) -> {
                                for (int ii = 0; ii < 100; ii++) blackHole.consume(row.get(i, Integer.class));
                                return 0;
                            }))
                            .single()
                            .flatMap(l -> {
                                latch.countDown();
                                return (Mono<Void>)connection.close();
                            })).subscribe();
        });
        latch.await();
    }
    @Benchmark
    public void Do1Virtual(MyState state, Blackhole blackHole) throws InterruptedException {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executeDo1(state, executor, blackHole);
        }
    }

    @Benchmark
    public void Do1Platform(MyState state, Blackhole blackHole) throws InterruptedException {
        try (var executor = Executors.newCachedThreadPool()) {
            executeDo1(state, executor, blackHole);
        }
    }


    @Benchmark
    public void Do1R2DBC(MyState state, Blackhole blackHole) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(state.numberOfTasks);
        IntStream.range(0, state.numberOfTasks).forEach(i -> {
            state.poolR2Dbc.create()
                    .flatMap(connection -> ((Flux<MariadbResult>) connection.createStatement("DO 1").execute())
                        .flatMap(it -> it.getRowsUpdated())
                        .single()
                        .flatMap(l -> {
                            latch.countDown();
                            return (Mono<Void>)connection.close();
                        })).subscribe();
        });
        latch.await();
    }

    private void executeDo1(MyState state, ExecutorService executor, Blackhole blackHole) throws InterruptedException {
        IntStream.range(0, state.numberOfTasks).forEach(i -> executor.submit(() -> {
            try (var conn = state.pool.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    blackHole.consume(stmt.executeUpdate("DO 1"));
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

    @Benchmark
    public void Select1000RowsR2DBC(MyState state, Blackhole blackHole) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(state.numberOfTasks);
        IntStream.range(0, state.numberOfTasks).forEach(i -> {
            state.poolR2Dbc.create()
                    .flatMap(connection -> ((Flux<MariadbResult>) connection.createStatement("select seq from seq_1_to_1000").execute())
                            .flatMap(it -> it.map((row, metadata) -> row.get(0)))
                            .then(Mono.defer(() -> {
                                latch.countDown();
                                return (Mono<Void>)connection.close();
                            }))).subscribe();
        });
        latch.await();
    }

    @State(Scope.Benchmark)
    public static class MyState {

        // connections
        protected HikariDataSource pool;
        protected ConnectionPool poolR2Dbc;

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
            config.setJdbcUrl(String.format("jdbc:%s://localhost:3306/testj", driver));
            config.setUsername("root");

            // in order to compare the same thing with mysql and mariadb driver,
            config.addDataSourceProperty("sslMode", "DISABLED");
            config.addDataSourceProperty("serverTimezone", "UTC");

            config.setMaximumPoolSize(numberOfConnection);
            config.setPoolName("foo");
            config.setRegisterMbeans(true);
            pool = new HikariDataSource(config);
            ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                    .option(DRIVER,"mariadb")
                    .option(HOST,"localhost")
                    .option(PORT,3306)
                    .option(USER,"root")
                    .option(DATABASE,"testj")
                    .build());
            System.out.println(connectionFactory.toString());
            // Create a ConnectionPool for connectionFactory
            ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                    .maxIdleTime(Duration.ofMillis(1000))
                    .maxSize(numberOfConnection)
                    .build();

            poolR2Dbc = new ConnectionPool(configuration);

            for (int i = 0; i < 100; i++) {
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                ObjectName poolName = new ObjectName("com.zaxxer.hikari:type=Pool (foo)");
                HikariPoolMXBean poolProxy = JMX.newMXBeanProxy(mBeanServer, poolName, HikariPoolMXBean.class);
                System.out.println("Total Connections: " + poolProxy.getTotalConnections() + " after " + (i * 0.1) + "s)");
                if (poolProxy.getTotalConnections() == numberOfConnection) break;
                // to ensure pool create all connections
                Thread.sleep(100);
            }
            List<Connection> connections = new ArrayList<>();
            for (int i = 0; i < numberOfConnection; i++) {
                connections.add(poolR2Dbc.create().block());
            }
            for (Connection connection : connections) {
                ((Mono<Void>)connection.close()).block();
            }
            System.out.println("r2dbc pool size" + poolR2Dbc.getMetrics().get().idleSize());
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws SQLException {
            pool.close();
            poolR2Dbc.dispose();
        }
    }

}