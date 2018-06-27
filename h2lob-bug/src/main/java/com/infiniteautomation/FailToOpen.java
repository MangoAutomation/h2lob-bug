/**
 * Copyright (C) 2018 Infinite Automation Software. All rights reserved.
 */
package com.infiniteautomation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;

/**
 * Simple test to show how opening an h2 database created by 1.4.196 fails
 * on 1.4.197 on the second attempt.
 *
 * @author Terry Packer
 */
public class FailToOpen {
    protected static final File baseTestDir = new File("junit");
    
    public static void main(String[] args) throws SQLException, IOException, InterruptedException {
        
        delete(baseTestDir);

        //Copy the old db created with 1.4.196
        File oldDb = new File(new File("data"), "h2-test.h2.db");
        File databases = new File(baseTestDir, "databases");
        databases.mkdirs();
        File newDb = new File(databases, "h2-test.h2.db");
        CopyOption[] options = new CopyOption[]{
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.COPY_ATTRIBUTES
              };
        Files.copy(oldDb.toPath(), newDb.toPath(), options);
        
        //Setup the data source
        JdbcDataSource jds = new JdbcDataSource();
        jds.setURL(getUrl());
        jds.setDescription("test");
        JdbcConnectionPool pool = JdbcConnectionPool.create(jds);
        
        //Create the tables for use in testing when not copying a 1.4.196 db
//        createTables(pool);
        
        
        //Open a connection and make a query
        testConnection(pool);
        insertData(pool);
        selectData(pool);
        pool.dispose();
        
        System.out.println("***************RESTARTING***************\n\n\n");
        //Open again and see failure
        jds = new JdbcDataSource();
        jds.setURL(getUrl());
        jds.setDescription("test");
        pool = JdbcConnectionPool.create(jds);

        testConnection(pool);
        insertData(pool);
        selectData(pool);
        pool.dispose();
    }
    
    /**
     * @param pool
     * @throws SQLException 
     */
    private static void createTables(JdbcConnectionPool pool) throws SQLException {
        //Create the test table
        Connection conn = pool.getConnection();
        conn.setAutoCommit(true);
        conn.createStatement().executeUpdate(getCreateDataSourcesSQL());
        conn.createStatement().executeUpdate(getCreateDataPointsSQL());
        conn.createStatement().executeUpdate(getCreateDataPointTagsSQL());
        conn.close();
    }

    /**
     * @param pool
     * @throws SQLException 
     */
    private static void testConnection(JdbcConnectionPool pool) throws SQLException {
        Connection conn = pool.getConnection();
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE table_name='dataPoints' AND table_schema='PUBLIC'");
        while(rs.next()) {
            System.out.println("count: " + rs.getInt(1));
        }
        conn.close();
    }

    /**
     * @param pool
     * @throws SQLException 
     */
    private static void selectData(JdbcConnectionPool pool) throws SQLException {
        Connection conn = pool.getConnection();
        ResultSet rs = conn.createStatement().executeQuery("SELECT tagKey,tagValue FROM dataPointTags");
        while(rs.next()) {
            System.out.println("key: " + rs.getString(1));
            System.out.println("value: " + rs.getString(1));
        }
        conn.close();
    }

    /**
     * @param pool
     * @throws SQLException 
     */
    private static void insertData(JdbcConnectionPool pool) throws SQLException {
        Connection conn = pool.getConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("INSERT INTO dataSources (value) VALUES (1)", Statement.RETURN_GENERATED_KEYS);
        ResultSet rs = stmt.getGeneratedKeys();
        rs.next();
        int dsId = rs.getInt(1);

        stmt = conn.createStatement();
        stmt.executeUpdate("INSERT INTO dataPoints (dataSourceId,value) VALUES (" + dsId + ",2)", Statement.RETURN_GENERATED_KEYS);
        rs = stmt.getGeneratedKeys();
        rs.next();
        int dpId = rs.getInt(1);
        
        conn.createStatement().executeUpdate("INSERT INTO dataPointTags (dataPointId,tagKey,tagValue) VALUES (" + dpId + ",'KEY','VALUE')");
        conn.close();
    }

    public static String getUrl() {
        return "jdbc:h2:" + baseTestDir.getAbsolutePath() + "/databases/h2-test;MV_STORE=FALSE;TRACE_LEVEL_FILE=3";
    }
    
    public static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (f.exists() && !f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }
    
    public static String getCreateDataSourcesSQL() {
        return "CREATE TABLE dataSources (\n" + 
                "  id int NOT NULL auto_increment,\n" +
                "  value int NOT NULL,\n" +
                "  PRIMARY KEY (id)\n" + 
                ");\n";
    }
    
    public static String getCreateDataPointsSQL() {
        return "CREATE TABLE dataPoints (\n" + 
                "  id int NOT NULL auto_increment,\n" + 
                "  dataSourceId int NOT NULL,\n" + 
                "  value int NOT NULL,\n" +
                "  PRIMARY KEY (id)\n" + 
                ");\n" + 
                "ALTER TABLE dataPoints ADD CONSTRAINT dataPointsFk1 FOREIGN KEY (dataSourceId) REFERENCES dataSources(id);\n";
    }
    
    public static String getCreateDataPointTagsSQL() {
        return "CREATE TABLE dataPointTags (\n" + 
                "  dataPointId INT NOT NULL,\n" + 
                "  tagKey VARCHAR(255) NOT NULL,\n" + 
                "  tagValue VARCHAR(255) NOT NULL\n" + 
                ");\n" + 
                "ALTER TABLE dataPointTags ADD CONSTRAINT dataPointTagsUn1 UNIQUE (dataPointId ASC, tagKey ASC);\n" + 
                "ALTER TABLE dataPointTags ADD CONSTRAINT dataPointTagsFk1 FOREIGN KEY (dataPointId) REFERENCES dataPoints (id) ON DELETE CASCADE;\n";
    }
}
