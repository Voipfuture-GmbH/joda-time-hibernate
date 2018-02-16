/*
 *  Copyright 2001-2012 Stephen Colebourne
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.joda.time.contrib.hibernate;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.HSQLDialect;
import org.hibernate.jdbc.Work;

import junit.framework.TestCase;

public abstract class HibernateTestCase extends TestCase
{
    private SessionFactory factory;
    private Configuration cfg;

    protected SessionFactory getSessionFactory()
    {
        if (this.factory == null)
        {
            cfg = new Configuration();

            setupConfiguration(cfg);

            cfg.setProperty("hibernate.connection.driver_class", "org.hsqldb.jdbcDriver");
            cfg.setProperty("hibernate.connection.url", "jdbc:hsqldb:mem:hbmtest" + getClass().getName());
            cfg.setProperty("hibernate.dialect", HSQLDialect.class.getName());
            cfg.setProperty("hibernate.show_sql", "true");
            cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop" );
            cfg.setProperty("hibernate.connection.autocommit", "true" );

            SessionFactory factory = cfg.buildSessionFactory();
            this.factory = factory;
        }
        return factory;
    }

    private Session session;
    private Transaction transaction;
    
    protected Session newSession() {
        SessionFactory factory = getSessionFactory();
        this.session = factory.openSession();
        this.session.setHibernateFlushMode(FlushMode.ALWAYS);
        startTransaction();
        return session;
    }
    
    protected void startTransaction() {
        if ( this.session == null ) {
            newSession();
        }
        this.transaction = this.session.getTransaction();
        this.transaction.begin();
    }
    
    protected void commitTransaction() {
        this.transaction.commit();
        this.transaction = null;
    }

    protected void commitCurrentConnection(Session session) throws SQLException 
    {
        if ( this.transaction != null ) {
            this.transaction.commit();
        }
        doWithConnection( session, new Work() 
        {
            public void execute(Connection connection) throws SQLException {
                connection.commit();
            }
        });
    }

    protected void doWithConnection(Session session, Work work) throws SQLException {
        session.doWork( work );
    }

    protected void tearDown() throws Exception
    {
        if (this.factory != null)
        {
            try {
                this.factory.close();
            } finally {
                this.factory = null;
            }
        }
    }

    protected abstract void setupConfiguration(Configuration cfg);
}
