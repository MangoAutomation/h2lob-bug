/**
 * Copyright (C) 2018 Infinite Automation Software. All rights reserved.
 */
package com.infiniteautomation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;

/**
 * 
 * Simple test to generate a 1.4.196 h2 database
 *
 * @author Terry Packer
 */
public class GenerateData {
    protected static final File baseTestDir = new File("junit");
    
    
public static void main(String[] args) throws SQLException, IOException, InterruptedException {
        
        delete(baseTestDir);
        
        //Setup the data source
        JdbcDataSource jds = new JdbcDataSource();
        jds.setURL(getUrl());
        jds.setDescription("test");
        JdbcConnectionPool pool = JdbcConnectionPool.create(jds);
             
        //Create the test table
        Connection conn = pool.getConnection();
        conn.setAutoCommit(true);
        conn.createStatement().executeUpdate(getCreateDataSourcesSQL());
        conn.createStatement().executeUpdate(getCreateDataPointsSQL());
        conn.createStatement().executeUpdate(getCreateDataPointTagsSQL());
        conn.close();
        pool.dispose();
    }
    
    public static String getUrl() {
        return "jdbc:h2:" + baseTestDir.getAbsolutePath() + "/databases/h2-test;MV_STORE=FALSE;";
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
                "dataSourceId int NOT NULL,\n" + 
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
    
    
    
    public static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (f.exists() && !f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }
}
