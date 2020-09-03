package query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors
import java.io.BufferedWriter;

public class Postgres{
	private String url = "jdbc:postgresql://localhost:5432/testdb";
    private String user = "postgres";
    private String password = "postgres";
	private Connection conn;
	
	public Postgres(String url, String user, String password){
		this.url = url;
		this.user = user;
		this.password = password;
		
		// connect to db
		try{
			conn = connect();
			System.out.println("Connected to database .");
		}catch(SQLException ex){
			System.out.println(ex.getMessage());
		}catch(ClassNotFoundException ex){
			System.out.println(ex.getMessage());
		}
	}
	
	/**
     * Connect to the PostgreSQL database
     *
     * @return a Connection object
     * @throws java.sql.SQLException
     */
    public Connection connect() throws SQLException, ClassNotFoundException {
		Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection(url, user, password);
    }
	
	/*
		Export data of a given table .
	*/
	public boolean exportTable(String schemaName, String tableName){
		String fileName = schemaName + "." + tableName + ".txt";
		BufferedWriter file = null;
		
		// get column names
		String columns = getTableColumns(schemaName, tableName);
		System.out.println(columns);
		
		// get columns for select query
		String columnsForSelect = getColumnsForSelect(schemaName, tableName);
		
		// form insert query
		String insertQuery = "insert into " + schemaName + "." + tableName + "(" + columns + ")" + " values " + "(";
		
		// get all rows from table into resultset
		String sql = "select " + columnsForSelect + " from " + schemaName + "." + tableName;
		try{
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			System.out.println("Executed query : " + sql);
			
			file = new BufferedWriter(new FileWriter(fileName));
			while(rs.next()){
				// form insert query
				file.write(insertQuery + rs.getString(1) + ");\n");
				// write to a file
			}
			
			file.close();
		}catch(SQLException ex){
			System.out.println(ex.getMessage());
		}
		catch(IOException ex){
			ex.getMessage();
		}
		// loop over each row
			// create a dynamic insert statement for each row
		
			// write in a file
		
		return true;
	}
	
	/*
		Export all tables of given schema 
	*/
	public boolean exportSchemaTables(String schemaName){
		// get a list of all tables present in given schema
		
		return true;
	}
	
	/* Get column names of given table
	*/
	private String getTableColumns(String schema, String table){
		// create a list of column
		String columns = "";
		String sql = "SELECT column_name "
						+ "FROM information_schema.columns " 
						+ "WHERE table_schema = '" + schema + "'" 
						+ "AND table_name   = '" + table + "' ";
		
		try{
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			System.out.println("Executed query : " + sql);
			// concate the columns
			while(rs.next()){
				columns += rs.getString("column_name");
				columns += ", ";
			}
			columns = columns.substring(0, columns.length() - ", ".length());
			System.out.println(columns);
		}catch(SQLException ex){
			System.out.println(ex.getMessage());
		}
		return columns;
	}
	
	private String getColumnsForSelect(String schema, String table){
		String columns = "";
		String sql = "SELECT column_name, data_type "
						+ "FROM information_schema.columns " 
						+ "WHERE table_schema = '" + schema + "' "
						+ "AND table_name   = '" + table + "' ";
	
		try{
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			System.out.println("Executed query : " + sql);
			
			String column_name = null, data_type = null;
			while(rs.next()){
				column_name = rs.getString("column_name");
				data_type = rs.getString("data_type");
				
				if(data_type.equalsIgnoreCase("text") || data_type.equalsIgnoreCase("character varying")){		// add single quote around value
					columns = columns + " ''''" + " || " + column_name + " || " + " '''' ";
				}
				else{
					columns = columns + column_name;
				}
				columns += " || ', ' || ";
			}
			
			// remove last delimiter
			columns = columns.substring(0, columns.length() - " || ', ' || ".length());
			System.out.println(columns);
		}catch(SQLException ex){
			System.out.println(ex.getMessage());
		}
		return columns;
	}
	/**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
		String schema = "bank", table = "customer";
        Postgres pg = new Postgres("jdbc:postgresql://localhost:5432/testdb", "postgres", "postgres");
        pg.exportTable(schema, table);
    }
}
