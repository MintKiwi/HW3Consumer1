
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

public class Consumer {
    public static ConcurrentHashMap<String, int[]> map = new ConcurrentHashMap<>();
    final static private int NUMTHREADS = 200;
    private static String host = "54.244.11.31";
    public static void main(String[] args) throws Exception {

        Connection connection = Consumer.getConnection();
        ExecutorService pool = Executors.newFixedThreadPool(NUMTHREADS);
        for (int i = 0; i < NUMTHREADS; i++) {
            pool.execute(new MessageDispatcher(connection));

        }



    }


    public static Connection getConnection() throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(host);
        factory.setPort(5672);

        factory.setUsername("admin");
        factory.setPassword("admin");
        Connection connection = factory.newConnection();
        return connection;
    }

}
