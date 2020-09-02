package query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Postgres {
	private String url = "jdbc:postgresql://localhost:5432/postgres";
	private String user = "postgres";
	private String password = "postgres";
	
	private Connection conn;
	public Postgres(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
		
		conn = connect();
	}
	
	public Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }
	
	public boolean createSampleTable(String name) {
		String sql = "CREATE TABLE " + name + " ("
				+ "user_id serial PRIMARY KEY,"
				+ "username VARCHAR ( 50 ) UNIQUE NOT NULL,"
				+ "password VARCHAR ( 50 ) NOT NULL,"
				+ "email VARCHAR ( 255 ) UNIQUE NOT NULL,"
				+ "created_on TIMESTAMP NOT NULL,"
			    +    "last_login TIMESTAMP" 
			    + " )";
		
		try {
			Statement stmt = conn.createStatement();
			stmt.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		return true;
	}
	
	public boolean insertSampleData() {
		String sql1 = "INSERT INTO public.accounts\n" + 
				"(username, \"password\", email, created_on, last_login)\n" + 
				"VALUES('u1', 'p1', 'e1@gmail.com', current_timestamp, current_timestamp)";
		String sql2 = "INSERT INTO public.accounts\n" + 
				"(username, \"password\", email, created_on, last_login)\n" + 
				"VALUES('u2', 'p2', 'e2@gmail.com', current_timestamp, current_timestamp)";
		
		try {
			Statement stmt = conn.createStatement();
			//stmt.executeQuery(sql1);
			stmt.executeQuery(sql2);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		
		return true;
	}
	
	public boolean exportTable(String schema, String table) {
		String sql = "SELECT column_name\n" + 
				"  FROM information_schema.columns\n" + 
				" WHERE table_schema = '" + schema + "'" + 
				"   AND table_name   = '" + table + "'";
		
		// column names
		String columns = "", selectColumn = "";
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();	
			rs = stmt.executeQuery(sql);
			
			// concate columns
			while(rs.next()) {
				columns += rs.getString("column_name");
				columns += ", ";
				
				selectColumn += rs.getString("column_name");
				selectColumn += " || ', ' || ";
			}
			columns = columns.substring(0,  columns.length() - ", ".length());
			selectColumn = selectColumn.substring(0, selectColumn.length() - " || ', ' || ".length());
			System.out.println(columns);
			System.out.println(selectColumn);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}
		
		
		String insertStatement = "insert into " + schema + "." + table + " ( ";
		insertStatement += columns;
		insertStatement += ") ";
		insertStatement += "values ( ";
		
		// select all rows
		String selectQuery = "select " + selectColumn + " from " + schema + "." + table;
		
		String row = "";
		try {
			rs = stmt.executeQuery(selectQuery);
			while(rs.next()) {
				row = insertStatement + rs.getString(1) + " ); ";
				System.out.println(row);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}
	
	public boolean exportSchema(String schema) {
		return true;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String url = "jdbc:postgresql://localhost:5432/postgres";
		String user = "postgres";
		String password = "postgres";
		Postgres pg = new Postgres(url, user, password);
		//pg.createSampleTable("accounts");
		//pg.insertSampleData();
		pg.exportTable("public", "accounts");
	}

}
