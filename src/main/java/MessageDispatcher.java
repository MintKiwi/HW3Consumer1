
import com.rabbitmq.client.*;
import dao.DBCPDataSource;
import dao.NumsDao;
import dao.SwipeDao;

import model.SwipePOJO;


import javax.sql.DataSource;
import java.io.IOException;



public class MessageDispatcher implements Runnable {


    private Connection connection;
    private DataSource dataSource;
    private NumsDao numsDao;
    private Consumer consumer;
    private SwipeDao swipeDao;


    public MessageDispatcher(Connection connection, Consumer consumer) {
        try {
            //connection factory pool for rabbitmq
            this.connection = connection;
            //datasource pool for mysql
            this.dataSource = DBCPDataSource.getDataSource();
            this.numsDao = NumsDao.getInstance();
            this.consumer = consumer;
            this.swipeDao = SwipeDao.getInstance();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void run() {
        try {
            Channel channel = connection.createChannel();
            channel.basicConsume("direct", true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    int index = message.indexOf("swiper");
                    int index2 = message.indexOf("swipee");
                    int index3 = message.indexOf("comment");
                    String swiper = message.substring(index + 9, index2 - 3);
                    String swipee = message.substring(index2 + 9, index3 - 3);
                    int index4 = message.indexOf("leftOrRight");
                    String direction = message.charAt(index4 + 14) == 'r' ? "right" : "left";
                    //currentCount is the old value
                    int currentCount = consumer.getCount().getAndIncrement();

                    //add data into the sets
                    if (direction.equals("left")) {
                        switch((currentCount / 100000) % 2){
                            case 0:
                                Consumer.unmatchedSet1.add(new SwipePOJO(Integer.parseInt(swipee), Integer.parseInt(swiper)));
                                break;
                            case 1:
                                Consumer.unmatchedSet2.add(new SwipePOJO(Integer.parseInt(swipee), Integer.parseInt(swiper)));
                                break;

                        }


                    }
                    //currentCount + 1 is the actual size，insert every 100000 rows in batch
                    if((currentCount + 1) % 100000 == 0){
                        switch ((currentCount / 100000) % 2){
                            case 0:
                                swipeDao.createSwipes(Consumer.unmatchedSet1);
                                Consumer.unmatchedSet1.clear();
                                break;
                            case 1:
                                swipeDao.createSwipes(Consumer.unmatchedSet2);
                                Consumer.unmatchedSet2.clear();
                                break;


                        }

                    }


                }

            });

        } catch (IOException e) {

            throw new RuntimeException(e);
        }


    }
}


