package dao;

import model.SwipePOJO;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SwipeDao {
    private static BasicDataSource dataSource;
    // Single pattern: instantiation is limited to one object.
    private static SwipeDao instance = null;


    public SwipeDao() {
        dataSource = DBCPDataSource.getDataSource();
    }


    public static SwipeDao getInstance() {
        if (instance == null) {
            instance = new SwipeDao();
        }
        return instance;
    }

    //create a SwipePOJO record
    public void createSwipe(SwipePOJO swipePOJO) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        String insertQueryStatement = "INSERT INTO Swipe (Swipee, Swiper) " +
                "VALUES (?,?)";
        try {
            conn = dataSource.getConnection();
            preparedStatement = conn.prepareStatement(insertQueryStatement);
            preparedStatement.setInt(1, swipePOJO.getSwipee());
            preparedStatement.setInt(2, swipePOJO.getSwiper());

            
            // execute insert SQL statement
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    //create SwipePOJO records
    public void createSwipes(Set<SwipePOJO> matchSet) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        String insertQueryStatement = "INSERT ignore INTO DisLikes (Swipee, Swiper) " +
                "VALUES (?,?)";
        try {
            conn = dataSource.getConnection();
            preparedStatement = conn.prepareStatement(insertQueryStatement);
            for(SwipePOJO swipePOJO : matchSet){
                preparedStatement.setInt(1, swipePOJO.getSwipee());
                preparedStatement.setInt(2, swipePOJO.getSwiper());
                preparedStatement.addBatch();

            }



            // execute insert SQL statement
            preparedStatement.executeBatch();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }


    /**
     * Get the SwipePOJO records by fetching them from your MySQL instance.
     * This runs a SELECT statement and returns a single SwipePOJO instance.
     */
    public List<SwipePOJO> getSwipePOJOBySwipee(String swipee) throws SQLException {
        List<SwipePOJO> swipePOJOs = new ArrayList<SwipePOJO>();
        String selectSwipePOJO = "SELECT Id, Swipee, Swiper FROM Swipe WHERE Swipee=?;";
        Connection connection = null;
        PreparedStatement selectStmt = null;
        ResultSet results = null;
        try {
            connection = dataSource.getConnection();
            selectStmt = connection.prepareStatement(selectSwipePOJO);
            selectStmt.setString(1, swipee);

            results = selectStmt.executeQuery();

            while (results.next()) {
                int id = results.getInt("Id");
                int swipee1 = results.getInt("Swipee");
                int swiper = results.getInt("Swiper");
                SwipePOJO swipePOJO = new SwipePOJO(id, swipee1, swiper);
                swipePOJOs.add(swipePOJO);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (selectStmt != null) {
                selectStmt.close();
            }
            if (results != null) {
                results.close();
            }
        }
        return swipePOJOs;
    }



    public void test(Set<Integer> matchSet) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        String insertQueryStatement = "INSERT INTO Count (count, coun) " +
                "VALUES (?,?)";
        try {
            conn = dataSource.getConnection();
            preparedStatement = conn.prepareStatement(insertQueryStatement);
            for(int i : matchSet){
                preparedStatement.setInt(1, i);
                preparedStatement.setInt(2, i);
                preparedStatement.addBatch();

            }



            // execute insert SQL statement
            preparedStatement.executeBatch();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }
}
