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

/*
Purpose : This program exports data from oracle database table , 
which will be inserted into a postgres database .

Input : Oracle database connection url,
		Schema name
		Table name
Output : Sql file , which has all rows of table in form of insert query .

ex : insert into TBAADM.ACCOUNT_LBL_RELTN_TBL(ACCT_LABEL, ACID, ENTITY_CRE_FLG, DEL_FLG, ACCT_LABEL_RELTN_DESC, LCHG_USER_ID, LCHG_TIME, RCRE_USER_ID, RCRE_TIME, TS_CNT, BANK_ID) 
values ('ACC', '_117097', 'Y', 'N', 'ACC LABEL CODE', 'C227803', to_timestamp('16/01/2015 14:28:00', 'dd/mm/yyyy hh24:mi:ss'), 'E182143', to_timestamp('16/01/2015 14:26:58', 'dd/mm/yyyy hh24:mi:ss'), 1, '01');

- All rows will be written in .sql file from where this script is executed .

*/
public class Oracle{
	//private String url = "jdbc:postgresql://localhost:5432/testdb";
	//private String user = "postgres";
    //private String password = "postgres";
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
			
			// set define off
			
			while(rs.next()){
				// form insert query
				file.write(insertQuery + rs.getString(1) + ");\n");
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
					columns = columns + column_name;
				}
				columns += " || ', ' || ";
			}
			
			// remove last delimiter
			columns = columns.substring(0, columns.length() - " || ', ' || ".length());
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
