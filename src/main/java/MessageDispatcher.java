
import com.rabbitmq.client.*;
import dao.DBCPDataSource;
import dao.NumsDao;
import model.NumsPOJO;


import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.sql.PreparedStatement;


public class MessageDispatcher implements Runnable {


    private Connection connection;
    private DataSource dataSource;
    private NumsDao numsDao;


    public MessageDispatcher(Connection connection) {
        try {
            //connection factory pool for rabbitmq
            this.connection = connection;
            //datasource pool for mysql
            this.dataSource = DBCPDataSource.getDataSource();
            this.numsDao = NumsDao.getInstance();

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
                    ConcurrentHashMap<String, int[]> map = Consumer.map;
                    int index = message.indexOf("swiper");
                    int index2 = message.indexOf("swipee");
                    int index3 = message.indexOf("comment");
                    String swiper = message.substring(index + 9, index2 - 3);
                    String swipee = message.substring(index2 + 9, index3 - 3);
                    int index4 = message.indexOf("leftOrRight");
                    String direction = message.charAt(index4 + 14) == 'r' ? "right" : "left";
                    //if the swiper is not in the map
                    if (!map.containsKey(swiper)) {
                        if (direction.equals("left")) {
//                            new int[]{numdislikes, numlikes}
                            map.put(swiper, new int[]{1, 0});
                        } else {
                            map.put(swiper, new int[]{0, 1});
                        }
                        // create a new record if the swiper is not created before
                        NumsPOJO numsPOJO = new NumsPOJO(Integer.parseInt(swiper), map.get(swiper)[1], map.get(swiper)[0]);
                        numsDao.createNums(numsPOJO);
                    } else {
                        // update the numdislikes field
                        if (direction.equals("left")) {
                            map.get(swiper)[0] += 1;
                            NumsPOJO numsPOJO = new NumsPOJO(Integer.parseInt(swiper), map.get(swiper)[1], map.get(swiper)[0]);
                            try {
                                numsDao.updateNumDislikes(numsPOJO);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                            // update the numlikes field
                        } else {
                            map.get(swiper)[1] += 1;
                            NumsPOJO numsPOJO = new NumsPOJO(Integer.parseInt(swiper), map.get(swiper)[1], map.get(swiper)[0]);
                            try {
                                numsDao.updateNumLikes(numsPOJO);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }


                    }


                }

            });


        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }
}


