package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UserDaoHibernateImpl implements UserDao {

    private final static Logger logger = LoggerFactory.getLogger(UserDaoHibernateImpl.class);

    private final SessionFactory factory = new Util().getSessionFactory();

    private final static String T_CREATE = "CREATE TABLE users (" +
            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
            "name VARCHAR(255) NOT NULL, " +
            "lastName VARCHAR(255) NOT NULL, " +
            "age TINYINT NOT NULL" +
            ");";

    @Override
    public void createUsersTable() {
        try (Session session = factory.openSession()) {
            session.beginTransaction();
            session.createNativeQuery(T_CREATE).executeUpdate();
            session.getTransaction().commit();
        } catch (Exception e) {
            logger.error("Ошибка при создании таблицы 'users': {}", e.getMessage(), e);
        }
    }

    @Override
    public void dropUsersTable() {
        try (Session session = factory.openSession()) {
            session.beginTransaction();
            session.createNativeQuery("DROP TABLE  if exists Users").executeUpdate();
            session.getTransaction().commit();
        } catch (Exception e) {
            logger.error("Ошибка при удалении таблица 'users': {}", e.getMessage(), e);
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        Transaction tr = null;
        try (Session session = factory.openSession()) {
            tr = session.beginTransaction();
            User user = new User(name, lastName, age);
            session.save(user);
            tr.commit();
        } catch (HibernateException he) {
            if (tr != null) {
                tr.rollback();
            }
            logger.error("Ошибка транзакции при создании пользователя: {}", he.getMessage(), he);
        } catch (Exception e) {
            logger.error("Неизвестная ошибка при создании пользователя {} {}: {}", name, lastName, e.getMessage(), e);
        }
    }

    @Override
    public void removeUserById(long id) {
        Transaction tr = null;
        try (Session session = factory.openSession()) {
            tr = session.beginTransaction();
            session.createQuery("delete from User where id = :userId")
                    .setParameter("userId", id)
                    .executeUpdate();
            tr.commit();
        } catch (HibernateException he) {
            if (tr != null) {
                tr.rollback();
            }
            logger.error("Ошибка транзакции при удалении пользователя: {}", he.getMessage(), he);
        } catch (Exception e) {
            logger.error("Ошибка при удалении пользователя с id = {}: {}", id, e.getMessage(), e);
        }
    }

    @Override
    public List<User> getAllUsers() {
        Transaction tr = null;
        List<User> users = new ArrayList<>();
        try (Session session = factory.openSession()) {
            tr = session.beginTransaction();
            users = session.createQuery("from User", User.class)
                    .getResultList();
            tr.commit();
        } catch (HibernateException he) {
            if (tr != null) {
                tr.rollback();
            }
            logger.error("Ошибка транзакции при получении пользователей: {}", he.getMessage(), he);
        } catch (Exception e) {
            logger.error("Ошибка при получении пользователей из БД! : {}", e.getMessage(), e);
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        Transaction tr = null;
        try (Session session = factory.openSession()) {
            tr = session.beginTransaction();
            session.createQuery("delete from User").executeUpdate();
            tr.commit();
        } catch (HibernateException he) {
            if (tr != null) {
                tr.rollback();
            }
            logger.error("Ошибка транзакции при удалении записей таблицы: {}", he.getMessage(), he);
        } catch (Exception e) {
            logger.error("Ошибка при удалении записей таблицы 'users': {}", e.getMessage(), e);
        }
    }

}
