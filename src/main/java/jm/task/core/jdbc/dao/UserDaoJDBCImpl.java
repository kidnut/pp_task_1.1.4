package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {

    }

    private final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS pre_project.users (\n" +
            "  id BIGINT(200) NOT NULL AUTO_INCREMENT,\n" +
            "  name VARCHAR(45) NOT NULL,\n" +
            "  lastName VARCHAR(45) NOT NULL,\n" +
            "  age TINYINT(3) NOT NULL,\n" +
            "  PRIMARY KEY (id))";
    private final String DROP_TABLE = "DROP TABLE IF EXISTS pre_project.users";
    private final String INSERT_USERS = "INSERT INTO pre_project.users (name, lastName, age) VALUES (?, ?, ?)";
    private final String SELECT_ALL = "SELECT * FROM pre_project.users";
    private final String DELETE = "DELETE FROM pre_project.users WHERE id = ? IS NULL OR id = ''";
    private final String CLEAN = "TRUNCATE TABLE pre_project.users";

    public void createUsersTable() {
        try (Connection connection = Util.getConnection();
             PreparedStatement ps = connection.prepareStatement(CREATE_TABLE);) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void dropUsersTable() {
        try (Connection connection = Util.getConnection();
             PreparedStatement ps = connection.prepareStatement(DROP_TABLE);) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try (Connection connection = Util.getConnection();
             PreparedStatement ps = connection.prepareStatement(INSERT_USERS);) {
            ps.setString(1, name);
            ps.setString(2, lastName);
            ps.setByte(3, age);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void removeUserById(long id) {
        try (Connection connection = Util.getConnection();
             PreparedStatement ps = connection.prepareStatement(DELETE);) {
            ps.setLong(1, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (Connection connection = Util.getConnection();
             PreparedStatement ps = connection.prepareStatement(SELECT_ALL);
             ResultSet rs = ps.executeQuery();) {
            while (rs.next()) {
                User user = new User();
                user.setId(rs.getLong("id"));
                user.setName(rs.getString("name"));
                user.setLastName(rs.getString("lastName"));
                user.setAge(rs.getByte("age"));
                users.add(user);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return users;
    }

    public void cleanUsersTable() {
        try (Connection connection = Util.getConnection();
             PreparedStatement ps = connection.prepareStatement(CLEAN);) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
