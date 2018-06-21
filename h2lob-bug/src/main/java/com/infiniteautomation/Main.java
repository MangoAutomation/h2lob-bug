/**
 * Copyright (C) 2018 Infinite Automation Software. All rights reserved.
 */
package com.infiniteautomation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;

/**
 * Example to show how the database size grows dramatically from updating a longblob column.
 *
 * @author Terry Packer
 */
public class Main {
    protected static final File baseTestDir = new File("junit");
    
    public static void main(String[] args) throws SQLException, IOException {
        
        delete(baseTestDir);
        
        //Setup the data source
        JdbcDataSource jds = new JdbcDataSource();
        jds.setURL(getUrl());
        jds.setDescription("test");
        JdbcConnectionPool pool = JdbcConnectionPool.create(jds);
             
        //Create the test table
        Connection conn = pool.getConnection();
        conn.setAutoCommit(true);
        conn.createStatement().executeUpdate("CREATE TABLE test (id int NOT NULL auto_increment, data longblob, PRIMARY KEY (id));");
        conn.close();
        
        //Get a handle on the file to check its size later
        File dbFile = new File(baseTestDir, "databases" + File.separator + "h2-test.h2.db");
        if(!dbFile.exists()) {
            throw new IOException("Database doesn't exist");
        }
        
        //Insert a row to update
        conn = pool.getConnection();
        conn.setAutoCommit(true);
        int id = conn.createStatement().executeUpdate("INSERT INTO test (data) VALUES (NULL)");
        conn.close();

        //Generate the data
        Map<Integer, Long> data = new HashMap<>(10000);
        for(int i=0; i<10000; i++) {
            data.put(i, 0L);
        }
        Map<String, Object> rtData = new HashMap<String, Object>();
        rtData.put("RT_DATA", data);
        
        //Update the row and watch the database size grow during runtime
        for(int i=0; i<200; i++) {
            for(Entry<Integer, Long> entry : data.entrySet())
                entry.setValue(entry.getValue() + 1L);
            updateRow(id, rtData, pool.getConnection());
            if(i%10 == 0) {
                long dbSize = dbFile.length();
                System.out.println(bytesDescription(dbSize));
            }
        }
    
        pool.dispose();
    }
    
    public static String getUrl() {
        return "jdbc:h2:" + baseTestDir.getAbsolutePath() + "/databases/h2-test;MV_STORE=FALSE";
    }

    public static void updateRow(int id, Map<String, Object> rtData, Connection conn) throws SQLException, IOException {
        conn.setAutoCommit(true);
        PreparedStatement ps = conn.prepareStatement("UPDATE test SET data=? WHERE id=?");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(rtData);
        oos.flush();
        oos.close();
        InputStream is = new ByteArrayInputStream(baos.toByteArray());
        ps.setBinaryStream(1, is);
        ps.setInt(2, id);
        ps.executeUpdate();
        ps.close();
        conn.close();
    }
    
    public static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (f.exists() && !f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }
    
    public static String bytesDescription(long size) {
        String sizeStr;
        if (size < 1028)
            sizeStr = size + " B";
        else {
            size /= 1028;
            if (size < 1000)
                sizeStr = size + " KB";
            else {
                size /= 1000;
                if (size < 1000)
                    sizeStr = size + " MB";
                else {
                    size /= 1000;
                    if (size < 1000)
                        sizeStr = size + " GB";
                    else {
                        size /= 1000;
                        sizeStr = size + " TB";
                    }
                }
            }
        }

        return sizeStr;
    }
}
