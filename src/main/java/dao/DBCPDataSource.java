package dao;

import org.apache.commons.dbcp2.BasicDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DBCPDataSource {
    private static BasicDataSource dataSource;
    private static Properties properties;



    static {
        // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html
        dataSource = new BasicDataSource();
        properties = new Properties();
        ClassLoader classLoader = DBCPDataSource.class.getClassLoader();
        InputStream is = classLoader.getResourceAsStream("jdbc.properties");
        try {
            properties.load(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String url = String.format("jdbc:mysql://%s:%s/%s?serverTimezone=UTC", properties.getProperty("MySQL_IP_ADDRESS"), properties.getProperty("MySQL_PORT"), properties.getProperty("DATABASE"));
        dataSource.setUrl(url);
        dataSource.setUsername(properties.getProperty("DB_USERNAME"));
        dataSource.setPassword(properties.getProperty("DB_PASSWORD"));
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(60);
    }

    public static BasicDataSource getDataSource() {
        return dataSource;
    }
}
