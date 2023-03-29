
import com.rabbitmq.client.*;
import dao.SwipeDao;
import model.SwipePOJO;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class Consumer {
    //store match lists

    public static ConcurrentHashMap<String, int[]> map = new ConcurrentHashMap<>();
    //store unmatched sets, two sets are required to avoid ConcurrentModificationException
    public static final Set<SwipePOJO> unmatchedSet1 = Collections.synchronizedSet(new HashSet<SwipePOJO>());
    public static final Set<SwipePOJO> unmatchedSet2 = Collections.synchronizedSet(new HashSet<SwipePOJO>());

    final static private int NUMTHREADS = 200;
    //rabbitmq host
    private static String host = "54.212.63.10";
    //record request count
    private static AtomicInteger count = new AtomicInteger(0);
    public AtomicInteger getCount() {
        return count;
    }





    public static void main(String[] args) throws Exception {

        Consumer obj = new Consumer();
        Connection connection = Consumer.getConnection();
        ExecutorService pool = Executors.newFixedThreadPool(NUMTHREADS);
        for (int i = 0; i < NUMTHREADS; i++) {
            pool.execute(new MessageDispatcher(connection, obj));

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
