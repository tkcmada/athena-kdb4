import java.sql.*;
import java.util.*;

public class TestKdbJdbc {
	public static void main(String[] args) throws Exception {
		Class.forName("jdbc");
		Connection h = DriverManager.getConnection("jdbc:q:127.0.0.1:5001", "", "");
		System.out.println("connected. v1");
		Statement stmt = h.createStatement();
		ResultSet rs = stmt.executeQuery("select * from t6");
		while(rs.next()) {
			System.out.println("---");
			Object obj = rs.getObject(1);
			if(obj == null)
				System.out.println("null");
			else if(obj.getClass().isArray()) {
				Object[] a = (Object[])obj;
				for(int i = 0; i < a.length; i++) {
					System.out.println(i + ":" + a[i] + ":" + (a[i] == null ? "null" : a[i].getClass().getName()));
				}
			}
			else
				System.out.println(obj + ":" + obj.getClass().getName());
		}
	}
}
