package jm.task.core.jdbc.util;
import jm.task.core.jdbc.model.User;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import java.util.Properties;
import org.hibernate.cfg.Environment;
import org.hibernate.service.ServiceRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

    private final static String URL = "jdbc:mysql://localhost:3306/mydatabase";
    private final static String USERNAME = "root";
    private final static String PASSWORD = "root";

    private SessionFactory sessionFactory;
    Logger logger = LoggerFactory.getLogger(Util.class);

    public SessionFactory getSessionFactory() {
        if (sessionFactory == null) {
            try {
                Configuration configuration = new Configuration();

                Properties settings = new Properties();
                settings.put(Environment.DRIVER, "com.mysql.cj.jdbc.Driver");
                settings.put(Environment.URL, URL);
                settings.put(Environment.USER, USERNAME);
                settings.put(Environment.PASS, PASSWORD);
                settings.put(Environment.DIALECT, "org.hibernate.dialect.MySQLDialect");

                settings.put(Environment.SHOW_SQL, "true");

                settings.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");

                configuration.setProperties(settings);

                configuration.addAnnotatedClass(User.class);

                ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
                        .applySettings(configuration.getProperties()).build();

                sessionFactory = configuration.buildSessionFactory(serviceRegistry);
            } catch (Exception e) {
                logger.error("Ошибка соединения: {}", e.getMessage(), e);
            }
        }
        return sessionFactory;
    }
}
