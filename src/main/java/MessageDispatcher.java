
import com.rabbitmq.client.*;


import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;


public class MessageDispatcher implements Runnable {


    private Connection connection;



    public MessageDispatcher(Connection connection) {

        this.connection = connection;
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
                    String swiper = message.substring(index + 9, index2 - 3);
                    int index4 = message.indexOf("leftOrRight");
                    String direction = message.charAt(index4 + 14) == 'r' ? "right" : "left";
                    if (!map.containsKey(swiper)) {
                        if (direction.equals("left")) {
                            map.put(swiper, new int[]{1, 0});
                        } else {
                            map.put(swiper, new int[]{0, 1});
                        }
                    } else {
                        if (direction.equals("left")) {
                            map.get(swiper)[0] += 1;
                        } else {
                            map.get(swiper)[1] += 1;
                        }

                    }

                }

            });



        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }
}


