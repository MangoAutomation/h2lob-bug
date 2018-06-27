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
             
        //Open a connection and make a query
        Connection conn = pool.getConnection();
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE table_name='DATAPOINTS' AND table_schema='PUBLIC'");
        while(rs.next()) {
            System.out.println("count: " + rs.getInt(1));
        }

        conn.close();
        pool.dispose();
        
        //Open again and see failure
        jds = new JdbcDataSource();
        jds.setURL(getUrl());
        jds.setDescription("test");
        pool = JdbcConnectionPool.create(jds);
        
        conn = pool.getConnection();
        rs = conn.createStatement().executeQuery("SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE table_name='USERS' AND table_schema='PUBLIC'");
        while(rs.next()) {
            System.out.println("count: " + rs.getInt(1));
        }
        
        
    }
    
    public static String getUrl() {
        return "jdbc:h2:" + baseTestDir.getAbsolutePath() + "/databases/h2-test;MV_STORE=FALSE";
    }
    
    public static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (f.exists() && !f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }
}
