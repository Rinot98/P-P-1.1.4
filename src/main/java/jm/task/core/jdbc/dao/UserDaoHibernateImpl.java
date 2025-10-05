package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UserDaoHibernateImpl implements UserDao {

    private final Logger logger = LoggerFactory.getLogger(UserDaoHibernateImpl.class);

    private final SessionFactory factory;

    private final String T_CREATE = "CREATE TABLE users (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "name VARCHAR(255) NOT NULL, " +
            "lastName VARCHAR(255) NOT NULL, " +
            "age TINYINT NOT NULL" +
            ");";

    public UserDaoHibernateImpl() {
//        factory = new Configuration()
//                .configure("hibernate.cfg.xml")
//                .addAnnotatedClass(User.class)
//                .buildSessionFactory();
        Util util = new Util();
        this.factory = util.getSessionFactory();
    }


    @Override
    public void createUsersTable() {
        Session session = null;
        try {
            session = factory.getCurrentSession();
            session.beginTransaction();
            session.createNativeQuery(T_CREATE).executeUpdate();
            session.getTransaction().commit();
        } catch (Exception e) {
            if (session.getTransaction() != null) {
                session.getTransaction().rollback();
            }
            logger.error("Ошибка при создании таблицы 'users': {}", e.getMessage(), e);
        }
    }

    @Override
    public void dropUsersTable() {
        Session session = null;
        try {
            session = factory.getCurrentSession();
            session.beginTransaction();
            session.createNativeQuery("DROP TABLE  if exists Users").executeUpdate();
            session.getTransaction().commit();
        } catch (Exception e) {
            if (session.getTransaction() != null) {
                session.getTransaction().rollback();
            }
            logger.error("Ошибка при удалении таблица 'users': {}", e.getMessage(), e);
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        Session session = null;
        try {
            session = factory.getCurrentSession();
            User user = new User(name, lastName, age);
            session.beginTransaction();
            session.save(user);
            session.getTransaction().commit();
        } catch (Exception e) {
            session.getTransaction().rollback();
            logger.error("Ошибка при создании пользователя {} {}: {}", name, lastName, e.getMessage(), e);
        }
    }

    @Override
    public void removeUserById(long id) {
        Session session = null;
        try {
            session = factory.getCurrentSession();
            session.beginTransaction();
            session.createQuery("delete from User where id = :userId")
                    .setParameter("userId", id)
                    .executeUpdate();
            session.getTransaction().commit();
        } catch (Exception e) {
            if (session.getTransaction() != null) {
                session.getTransaction().rollback();
            }
            logger.error("Ошибка при удалении пользователя с id = {}: {}", id, e.getMessage(), e);
        }
    }

    @Override
    public List<User> getAllUsers() {
        Session session = null;
        List<User> users = new ArrayList<>();
        try {
            session = factory.getCurrentSession();
            session.beginTransaction();
            users = session.createQuery("from User", User.class)
                    .getResultList();
            session.getTransaction().commit();
            users.forEach(System.out::println);
        } catch (Exception e) {
            if (session.getTransaction() != null) {
                session.getTransaction().rollback();
            }
            logger.error("Ошибка при получении пользователей из БД! : {}", e.getMessage(), e);
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        Session session = null;
        try {
            session = factory.getCurrentSession();
            session.beginTransaction();
            session.createQuery("delete from User").executeUpdate();
            session.getTransaction().commit();
        } catch (Exception e) {
            if (session.getTransaction() != null) {
                session.getTransaction().rollback();
            }
            logger.error("Ошибка при удалении записей таблицы 'users': {}", e.getMessage(), e);
        }
    }
}
