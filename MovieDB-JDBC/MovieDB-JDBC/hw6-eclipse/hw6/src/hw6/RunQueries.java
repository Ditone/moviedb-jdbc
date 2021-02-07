package hw6;

import java.sql.*;
import java.util.Scanner;

public class RunQueries {
	
	public static Connection conn;
	public static Statement stmt;
	
	public static void main(String args[]){
		
		//personal login
		setUpDatabase("qreidy", "cdm1355894");
		
		//will accept user input up until newline
		Scanner inputScan = new Scanner (System.in);
		
		//May add option to choose multiple options- depends on how long I have leftover
		System.out.println("The following query will return the top 10 movies names, "
				+ "and the number of times reviewed that they were reviewed for the "
				+ "profession you select.");
		
		//Loops so that there can be multiple executions of the query and/or multiple tests
		while (true){
			System.out.println("Please enter the job name which you would like to search for."
					+ "\n(? for a list of job titles || 0 to exit.)");
			
			
			String input = inputScan.nextLine();
			//testing 
			
			input = input.toUpperCase().trim();
			
			
			//ends the loop
			if (input.equals("0"))
				break;
			
			//prints the list of options then returns to the start of the loop
			else if (input.equals("?")){
				listOfOptions();
				continue;
			}
			
			else 
				queryOne(input);
		}
		
		//shut it all down
		inputScan.close();
		try{
			stmt.close();
			conn.close();
		}
		
		catch (SQLException se){
			System.out.println("Something went horribly wrong while closing the connections: " + se);
			System.exit(1);
		}
		
		System.out.println("Everything is closed. \nProgram exiting.");
		System.exit(0);
	}
	
	//Method to establish the database connection without having to bloat the main
	//Also helps with practice to isolate the database initiation code
	public static void setUpDatabase(String userId, String passWord){
		
		//Attempt to load the JDBC driver
		System.out.println ("Attempting to load the JDBC driver...");
		try {
			Class.forName("oracle.jdbc.OracleDriver");
		}
		
		catch(ClassNotFoundException e){
			System.out.println ("Something went horribly wrong with the driver: " + e);
			System.exit(1);
		}
		System.out.println ("Successfully loaded the driver.");
		
		//Attempt to connect to the database
		System.out.println ("Attemping to connect to the DePaul database...");
		try{
			conn = DriverManager.getConnection("jdbc:oracle:thin:@acadoradbprd01.dpu.depaul.edu:1521:ACADPRD0",
				userId, passWord);
			stmt=conn.createStatement();
		}
		
		catch (SQLException se){
			System.out.println("Something went horribly wrong while connecting to the database: " + se);
			System.exit(1);
		}
		System.out.println ("Successfully connected to the DePaul database.");
		
	}
	
	//Query that searches the database for the top 10 movies reviewed for a specific profession
	//Profession is loaded in via the parameter into a prepared statement
	public static void queryOne (String jobTitle){
		int count = 0;
		try{
			PreparedStatement prepQuer = conn.prepareStatement(""
					+ "SELECT movie_details.movie_title, COUNT(ratings.movie_id)\r\n" + 
					"	FROM  movie_details INNER JOIN (ratings INNER JOIN user_details \r\n" + 
					"			ON ratings.user_id = user_details.user_id)\r\n" + 
					"		ON movie_details.movie_id = ratings.movie_id\r\n" + 
					"	WHERE occupation = ( \r\n" + 
					"		SELECT job_code\r\n" + 
					"		FROM job_key\r\n" + 
					"		WHERE UPPER(job_translate) LIKE UPPER(?))\r\n" + 
					"	GROUP BY movie_details.movie_title\r\n" + 
					"	ORDER BY COUNT(ratings.movie_id) DESC\r\n");
			
			//Replaces the ? in the nested WHERE statement
			prepQuer.setString(1, jobTitle);
			
			//Result set doesn't need to be stored anywhere, so we'll just print it to the screen
			ResultSet rset = prepQuer.executeQuery();
			System.out.println ("\n\n-------------");
			System.out.println ("-------------");
			System.out.println ("-------------\n\n");
			System.out.println ("Top 10 reviewed movies for " + jobTitle.toLowerCase() + "s.");
			while (rset.next() && count < 10){
				count ++;
				System.out.println ("Movie: " + rset.getString("movie_title") + "| Times reviewed: " + rset.getString("COUNT(ratings.movie_id)"));
			}
			System.out.println ("\n\n-------------");
			System.out.println ("-------------");
			System.out.println ("-------------\n\n");
			rset.close();
		}
		catch (SQLException se){
			System.out.println ("Something went horribly wrong during the final query: " + se);
			System.exit(1);
		}
	}
	
	//Additional option in main
	//Simple query to return all the names of the jobs currently stored in the job_key table
	//Feels like a more elegant solution in case job_key table gets added to in the future
	public static void listOfOptions(){
		try{
			ResultSet rset = stmt.executeQuery("SELECT * FROM job_key");
			while (rset.next()){
				System.out.println (rset.getString ("job_translate"));
			}
		}
		catch(SQLException se){
			System.out.println("Something went horribly wrong with the options query: " + se);
			System.exit(1);
		}
		
		System.out.println ("\n\n-------------");
		System.out.println ("-------------");
		System.out.println ("-------------\n\n");
	}
	
	public static void showMovieTable() {
		int count = 0;
		try {
			ResultSet rset = stmt.executeQuery("SELECT * FROM movie_details");
			while (rset.next() && count < 20) {
				System.out.println ("Movie name: " + rset.getString("movie_title") + "| Movie Year: " + rset.getString("movie_year"));
				count ++;
			}
		}
		
		catch(SQLException se){
			System.out.println("Something went horribly wrong with the show query: " + se);
			System.exit(1);
		}
	}
}
