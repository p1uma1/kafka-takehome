package org.uor.takehome;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Database {

    private static final String URL =
            "jdbc:postgresql://localhost:5432/orderdb";

    private static final String USER = "postgres";

    private static final String PASSWORD = "mysecretpassword";

    public static Connection getConnection() throws SQLException {

        return DriverManager.getConnection(
                URL,
                USER,
                PASSWORD
        );
    }

    public static void saveOrder(Order order)
        throws SQLException {

    String sql = """
            INSERT INTO orders (order_id, product, price)
            VALUES (?, ?, ?)
            ON CONFLICT (order_id) DO NOTHING
            """;

    try (
        Connection connection = getConnection();
        PreparedStatement statement =
                connection.prepareStatement(sql)
    ) {

        statement.setString(
                1,
                order.getOrderId().toString()
        );

        statement.setString(
                2,
                order.getProduct().toString()
        );

        statement.setFloat(
                3,
                order.getPrice()
        );

        statement.executeUpdate();
    }
}

    public static void main(String[] args) {

        try (Connection connection = getConnection()) {

            System.out.println("Database connected!");

        } catch (SQLException e) {

            System.out.println(
                    "Database connection failed: "
                    + e.getMessage()
            );
        }
    }
}