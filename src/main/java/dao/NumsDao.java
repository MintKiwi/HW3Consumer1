package dao;

import model.NumsPOJO;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class NumsDao {
    private static BasicDataSource dataSource;
    // Single pattern: instantiation is limited to one object.
    private static NumsDao instance = null;

    public NumsDao() {
        dataSource = DBCPDataSource.getDataSource();
    }

    public static NumsDao getInstance() {
        if (instance == null) {
            instance = new NumsDao();
        }
        return instance;
    }

    //create a NumsPOJO record
    public void createNums(NumsPOJO numsPOJO) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        String insertQueryStatement = "INSERT INTO Nums (Swiper, NumDislikes, Numlikes) " +
                "VALUES (?,?,?)";
        try {
            conn = dataSource.getConnection();
            preparedStatement = conn.prepareStatement(insertQueryStatement);
            preparedStatement.setInt(1, numsPOJO.getSwiper());
            preparedStatement.setInt(2, numsPOJO.getNumdislikes());
            preparedStatement.setInt(3, numsPOJO.getNumlikes());


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
    //Get the NumsPOJO record by fetching it from your MySQL instance.
    public NumsPOJO getNumsPOJOBySwiper(int swiper) throws SQLException {
        String selectNumsPOJO = "SELECT Swiper, NumDislikes, Numlikes FROM Nums WHERE Swiper=?;";
        Connection connection = null;
        PreparedStatement selectStmt = null;
        ResultSet results = null;
        try {
            connection = dataSource.getConnection();
            selectStmt = connection.prepareStatement(selectNumsPOJO);
            selectStmt.setInt(1, swiper);
            results = selectStmt.executeQuery();
            if (results.next()) {
                int swiper1 = results.getInt("Swiper");
                int numdislike = results.getInt("NumDislikes");
                int numslikes = results.getInt("Numlikes");
                NumsPOJO numsPOJO = new NumsPOJO(swiper1, numslikes, numdislike);
                return numsPOJO;
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
        return null;
    }

    /**
     * Update the NumDislikes of the NumsPOJO instance.
     * This runs a UPDATE statement.
     */
    public NumsPOJO updateNumDislikes(NumsPOJO numsPOJO) throws SQLException {
        String NumsPOJO = "UPDATE Nums SET NumDislikes=? WHERE Swiper=?;";
        Connection connection = null;
        PreparedStatement updateStmt = null;
        try {
            connection = dataSource.getConnection();
            updateStmt = connection.prepareStatement(NumsPOJO);
            updateStmt.setInt(1, numsPOJO.getNumdislikes());
            updateStmt.setInt(2, numsPOJO.getSwiper());
            updateStmt.executeUpdate();



            return numsPOJO;
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (updateStmt != null) {
                updateStmt.close();
            }
        }
    }

    /**
     * Update the NumDislikes of the NumsPOJO instance.
     * This runs a UPDATE statement.
     */
    public NumsPOJO updateNumLikes(NumsPOJO numsPOJO) throws SQLException {
        String NumsPOJO = "UPDATE Nums SET Numlikes=? WHERE Swiper=?;";
        Connection connection = null;
        PreparedStatement updateStmt = null;
        try {
            connection = dataSource.getConnection();
            updateStmt = connection.prepareStatement(NumsPOJO);
            updateStmt.setInt(1, numsPOJO.getNumlikes());
            updateStmt.setInt(2, numsPOJO.getSwiper());
            updateStmt.executeUpdate();


            return numsPOJO;
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (updateStmt != null) {
                updateStmt.close();
            }
        }
    }


}
