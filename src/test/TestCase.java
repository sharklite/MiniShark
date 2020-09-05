package test;


import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestCase {

   @Test
    public void testNull() throws SQLException {
       Connection conn= DBS.getBasicDataSource().getConnection();
       PreparedStatement statement=conn.prepareStatement("select * from person where id=502");
       ResultSet rs=statement.executeQuery();
       while (rs.next()){
           Object name = rs.getObject("sex",int.class);
           System.out.println(name);
           System.out.println(rs.wasNull());
       }
       conn.close();
    }
}
