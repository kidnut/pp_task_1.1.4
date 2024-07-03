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

    private final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS " + Util.getSCHEMA() + "." + "users (\n" + "  id BIGINT(200) NOT NULL AUTO_INCREMENT,\n" + "  name VARCHAR(45) NOT NULL,\n" + "  lastName VARCHAR(45) NOT NULL,\n" + "  age TINYINT(3) NOT NULL,\n" + "  PRIMARY KEY (id))";
    private final String DROP_TABLE = "DROP TABLE IF EXISTS " + Util.getSCHEMA() + "." + "users";
    private final String INSERT_USERS = "INSERT INTO " + Util.getSCHEMA() + "." + "users (name, lastName, age) VALUES (?, ?, ?)";
    private final String SELECT_ALL = "SELECT * FROM " + Util.getSCHEMA() + "." + "users";
    private final String DELETE = "DELETE FROM " + Util.getSCHEMA() + "." + "users WHERE id = ?";
    private final String CLEAN = "TRUNCATE TABLE " + Util.getSCHEMA() + "." + "users";
    private final Connection connection = Util.getConnection();

    public UserDaoJDBCImpl() {

    }

    @Override
    public void createUsersTable() {
        try (PreparedStatement ps = connection.prepareStatement(CREATE_TABLE);) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropUsersTable() {
        try (PreparedStatement ps = connection.prepareStatement(DROP_TABLE);) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement ps = connection.prepareStatement(INSERT_USERS);) {
            try {
                connection.setAutoCommit(false);
                ps.setString(1, name);
                ps.setString(2, lastName);
                ps.setByte(3, age);
                ps.executeUpdate();
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeUserById(long id) {
        try (PreparedStatement ps = connection.prepareStatement(DELETE);) {
            try {
                connection.setAutoCommit(false);
                ps.setLong(1, id);
                ps.executeUpdate();
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(SELECT_ALL); ResultSet rs = ps.executeQuery();) {
            while (rs.next()) {
                User user = new User();
                user.setId(rs.getLong("id"));
                user.setName(rs.getString("name"));
                user.setLastName(rs.getString("lastName"));
                user.setAge(rs.getByte("age"));
                users.add(user);
            }
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println(users);
        return users;
    }

    @Override
    public void cleanUsersTable() {
        try (PreparedStatement ps = connection.prepareStatement(CLEAN);) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
