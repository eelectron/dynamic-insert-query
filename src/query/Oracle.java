package query;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors
import java.io.BufferedWriter;
/*
 * Author 	: 	Prashant
 * Date 	:	08-Sep-2020
 * Email	: 	prashant.2800163@edgeverve.com 
 * */
/*
Purpose : This program exports data from oracle database table , 
which will be inserted into a postgres database .

Input : Oracle database connection url,
		Schema name
		Table name
Output : Sql file , which has all rows of table in form of insert query .

- All rows will be written in .sql file on computer from where this program is executed .

*/

/*
 * Compile 	: javac Oracle.java
 * Run		: java -cp .:/path /to/oracle/jdbc/drive Oracle		
 * */
public class Oracle{
	private String url = "jdbc:oracle:thin:@10.66.118.22:1525/BMTDB";
	private String user = "dbread";
	private String password = "dbread";
	private Connection conn;
	
	public Oracle(String url, String user, String password){
		this.url = url;
		this.user = user;
		this.password = password;
		
		// connect to db
		try{
			conn = connect();
			System.out.println("Connected to database : " + url + " with user = " + user);
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
		//Class.forName("org.postgresql.Driver");
		Class.forName("oracle.jdbc.driver.OracleDriver");
        return DriverManager.getConnection(url, user, password);
    }
	
	/*
		Export data of a given table and write to a file in current folder .
		ex: insert into tableName(c1, c2, c3, ...) values (v1, 'v2', to_date('2020/09/04', 'yyyy/mm//dd'))
	*/
	public boolean exportTable(String schemaName, String tableName){
		String fileName = schemaName + "." + tableName + ".sql";
		BufferedWriter file = null;
		
		// get column names
		String columns = getTableColumns(schemaName, tableName);
		
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
			
			// create a new file 
			file = new BufferedWriter(new FileWriter(fileName));
			
			//truncate the table before inserting the rows
			file.write("truncate " + schemaName + "." + tableName + ";");
			file.write("\n\n");
			
			// use string builder
			// hold row
			String row = "";
			while(rs.next()){
				// form insert query
				row = rs.getString(1);
				
				// handle null values for string and numeric data type 
				row = row.replace(", ,", ", null,");
				
				file.write(insertQuery + row + ");\n");
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
		String input = "n";
		Scanner scanner = new Scanner(System.in);
		System.out.println("We are going to export all rows of each table of following schema : " + schemaName);
		System.out.println("If you want to continue with the process , then please enter y or Y otherwise enter n Or N");
		input = scanner.nextLine();
		if(input.equalsIgnoreCase("n") == true) {
			return false;
		}
		
		// get a list of all tables present in given schema
		String sql = "select table_name from all_tables where owner = '" + schemaName + "'";
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			System.out.println("Executed query : " + sql);
			
			String tableName = null;
			while(rs.next()) {
				tableName = rs.getString(1);
				exportTable(schemaName, tableName);
				System.out.println("exported table : " + tableName);
			}
		}catch(SQLException ex) {
			System.out.println(ex.getMessage());
		}
		return true;
	}
	
	/* Get column names of given table in the form "c1, c2, c3, ..."
	*/
	private String getTableColumns(String schema, String table){
		// create a list of column
		String columns = "";
		String sql = "SELECT column_name "
						+ "FROM all_tab_columns " 
						+ "WHERE owner = '" + schema + "'" 
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
			
			// remove last ,
			columns = columns.substring(0, columns.length() - ", ".length());
		}catch(SQLException ex){
			System.out.println(ex.getMessage());
		}
		return columns;
	}
	
	private String getColumnsForSelect(String schema, String table){
		String columns = "";
		String concat = " || ', ' || ";
		String sql = "SELECT column_name, data_type "
						+ "FROM all_tab_columns " 
						+ "WHERE owner = '" + schema + "' "
						+ "AND table_name   = '" + table + "' ";
	
		try{
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			System.out.println("Executed query : " + sql);
			
			String column_name = null, data_type = null;
			while(rs.next()){
				column_name = rs.getString("column_name");
				data_type = rs.getString("data_type");
				
				if(data_type.equalsIgnoreCase("char") || data_type.equalsIgnoreCase("varchar2")){		// add single quote around value if it is a string type
					columns = columns + " ''''" + " || " + "replace(" + column_name + ", '''', '''''')" + " || " + " '''' ";
				}
				else if(data_type.equalsIgnoreCase("date")){		// date 
					columns = columns + "'to_timestamp('''" + " || " + "to_char(" + column_name + ", 'dd/mm/yyyy hh24:mi:ss' )" + " || " + "''', ''dd/mm/yyyy hh24:mi:ss'')'";
				}
				else{
					columns = columns + "decode(" + column_name + ", NULL, 'null'," + column_name + ")";	// handle null numeric values
				}
				columns += concat;
			}
			
			// remove last delimiter
			columns = columns.substring(0, columns.length() - concat.length());
		}catch(SQLException ex){
			System.out.println(ex.getMessage());
		}
		return columns;
	}
	/**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
		String schema = "HR", table = "EMPLOYEES";
        //Oracle oracle = new Oracle("jdbc:oracle:thin:@10.66.118.22:1525/BMTDB", "dbread", "dbread");
		Oracle oracle = new Oracle("jdbc:oracle:thin:@localhost:1521/ORCLPDB1.localdomain", "system", "manager");
        //oracle.exportTable(schema, table);
        oracle.exportSchemaTables(schema);
    }
}
