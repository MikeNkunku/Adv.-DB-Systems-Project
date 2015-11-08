import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class PreAgregates {
	
	// implement the singleton pattern to make sure only one instance of this class is created
	private static PreAgregates instance = null;
	
	// private variables
	private int[] factors;		// the list of existing preagregate factors
	private String xAttribute, yAttribute, tableName;	// database metadata
	private int spacing;		// the space between two points in the dataset
	
	
	/**
	 * private constructor : use "create" method to instantiate a new instance ofPreAggregate
	 */
	private PreAgregates(int[] factors, String xAttribute, String yAttribute,
			String tableName, int spacing) {
		
		this.factors = factors;
		this.xAttribute = xAttribute;
		this.yAttribute = yAttribute;
		this.tableName = tableName;
		this.spacing = spacing;
	}
	
	/**
	 * get the unique instance of PreAgregates
	 */
	public static PreAgregates getInstance (Connection con, String xAttribute, 
			String yAttribute, String tableName, int maxResolution) {
		
		if ( instance == null ) {
			instance = create(con, xAttribute, yAttribute, tableName, maxResolution);
		}
		
		return instance;
	}

	/**
	 * create a new instance of PreAgregate class
	 * @param con
	 * @param xAttribute
	 * @param yAttribute
	 * @param tableName
	 * @param maxResolution
	 * @return
	 */
	private static PreAgregates create(Connection con, String xAttribute, 
			String yAttribute, String tableName, int maxResolution) {
		
		int pointWeight;	// the number of data entries represented by one point
		int spacing;		// the space between two points in the dataset
		int[] factorArray;	// the different preagregate factor present in database
		
		try {
			pointWeight = numberOfDataRowsPerPoints(con, tableName, maxResolution);  // Finds the number of rows in the table
			clusterTable(con, tableName, xAttribute);  //Checks if the table is clustered on the xAttribute
			spacing = calculateSpacing(con, tableName, xAttribute);  //Calculate the spacing between two points
			deletePreagregates(con, tableName);  //Delete any existing PreAgregates
			factorArray = createPreagregates(con, xAttribute, yAttribute, tableName, spacing, pointWeight);  // Create the PreAgregate Tables
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			return null;
		}
		
		return new PreAgregates(factorArray, xAttribute, yAttribute, tableName, spacing);
	}

	/**
	 * retrieve the number of data represented in one point
	 * @param con			: the database connection
	 * @param tableName		: the table name
	 * @param maxResolution	: the number of points displayed in the plot
	 * @throws SQLException
	 */
	private static int numberOfDataRowsPerPoints (Connection con, String tableName, int maxResolution) 
			throws SQLException
	{
		Statement st = con.createStatement();
		String query = "SELECT COUNT(*) FROM " + tableName + ";";
		ResultSet rs = st.executeQuery(query);
		
		if ( !rs.next())
			throw new SQLException("Couldn't count the number of rows of "+tableName+".\n");
		return (int) (rs.getInt(1) / maxResolution);
	}

	/**
	 * 
	 * @param con			: the database connection
	 * @param tableName		: the database table to cluster
	 * @param xAttribute	: the attribute on which we want to cluster the table
	 * @throws SQLException
	 */
	private static void clusterTable(Connection con, String tableName, String xAttribute) 
			throws SQLException
	{
		//Checks if the table is clustered on the xAttribute
		String query = "SELECT Count(*) FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid "+
		"AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r' AND t.relname = '"+tableName+"' AND a.attname = '"+xAttribute+"';";
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(query);
		if (!rs.next())
			throw new SQLException("Couldn't count the number of rows indexs of "+tableName+" on "+xAttribute+".\n");
		int nbrIndex = rs.getInt(1);
		if (nbrIndex == 0) {
			query = "CREATE INDEX "+ tableName+"_"+xAttribute+"Index" + " ON "
				  + tableName + " (" + xAttribute + ");";
			st.executeUpdate(query);
		}

		//Now cluster the table on the xAtribute
		query = "CLUSTER "+tableName+" USING "+tableName+"_"+xAttribute+"Index;";
		st.executeUpdate(query);
	}

	/**
	 * compute the spacing between two points
	 * @param con
	 * @param tableName
	 * @param xAttribute
	 * @return
	 * @throws Exception
	 */
	private static int calculateSpacing(Connection con, String tableName, String xAttribute) 
			throws SQLException
	{
		int pointI, pointII;
		String query = "SELECT "+xAttribute+" FROM "+tableName+" ORDER BY "+xAttribute+" ASC LIMIT 2;";
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(query);
		if ( !rs.next())
			throw new SQLException("Couldn't read the first row of "+tableName+".\n");
		pointI = (int) rs.getInt(1);
		if ( !rs.next())
			throw new SQLException("Couldn't read the first row of "+tableName+".\n");
		pointII = (int) rs.getInt(1);
		return pointII - pointI;
	}
	
	/**
	 * delete a preagregate table
	 * @param con
	 * @param tableName
	 * @throws SQLException
	 */
	private static void deletePreagregates(Connection con, String tableName) 
			throws SQLException
	{		
		Statement st = con.createStatement();
		Statement st2 = con.createStatement();
		String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '"+tableName+"pa%';";
		ResultSet rs = st.executeQuery(query);		
		while (rs.next())
		{
			String preagregateName = (String) rs.getString(1);
			query ="DROP TABLE "+preagregateName+";";
			st2.executeUpdate(query);
		}
	}

	/**
	 * create a preagregate table
	 * @param con
	 * @param xAttribute
	 * @param yAttribute
	 * @param tableName
	 * @param spacing
	 * @param qtdPoints
	 * @return
	 * @throws SQLException
	 */
	private static int[] createPreagregates(Connection con, String xAttribute, String yAttribute, String tableName, int spacing, int qtdPoints) 
			throws SQLException
	{
		int counter;
		int factor;
		Statement st = con.createStatement();
		String query;
		for (factor = 20, counter = 0; factor*10<= qtdPoints; factor = factor*2, counter++)
		{
			//To make this valid for every table it is necessary to make type independent. So it is not possible to hardcode average numeric(10,3) for example
			query = "CREATE TABLE " + tableName+"pa"+factor + "("+xAttribute +" bigint, average numeric(10,3), min_value numeric(10,3), max_value numeric(10,3));";
			st.executeUpdate(query);
			query = "INSERT INTO " + tableName+"pa"+factor + " SELECT div(" + xAttribute + "," + factor * spacing + ")*" +
				factor * spacing + ", avg(" + yAttribute + "), min(" + yAttribute + "), max(" + yAttribute + ") FROM " + tableName +
				" GROUP BY div("+xAttribute+","+factor*spacing+"); "+
				"CREATE INDEX "+tableName+"pa"+factor+"_xatributeIndex ON "+tableName+"pa"+factor+" ("+xAttribute+"); "+
				"CLUSTER "+tableName+"pa"+factor+" USING "+tableName+"pa"+factor+"_xatributeIndex;";
			st.executeUpdate(query);
		}

		//Here we save the factors on an array to pass to the method PreAgregates
		//There will be a problem here if the table has less than 12000 points, because counter will be zero
		int[] factorArray = new int[counter];
		for(int i = 20, ii = counter-1; i*10<= qtdPoints; i = i*2, ii--)
			factorArray[ii] = i;

		return factorArray;
	}
	
	/**
	 * choose the best factor for a given zoom
	 * @param trueFactor
	 * @return
	 */
	private int bestFactor(int trueFactor)
	{
		int usedFactor = 1;

		//Handling the special case of small factors. When they are big enoght to be slow, but not big enougth use 
		//preagregates acording to the normal rule. In this case we abuse the smallest of the preagregates.
		if( trueFactor > 2 * factors[factors.length-1] )
			usedFactor = factors[factors.length-1];

		//This checks wether the factor of the array is ate least ten times smaller than the trueFactor
		//If it is then this is the factor that should be used
		for(int i=0; i < factors.length; i++)
			if(trueFactor > factors[i]*5)
			{
				usedFactor = factors[i];
				break;
			}

		return usedFactor; 
	}
	
	/**
	 * create a SQL statement using the best preagregate
	 * @param start
	 * @param extent
	 * @param factor
	 * @return
	 */
	public String createStatement(long start, long extent, long factor) {
		int trueFactor = (int) factor/spacing;
		int usedFactor = bestFactor(trueFactor);

		//Generate the querry string and print what would be the true factor and the used preagregate.
		System.out.print(", true factor:  "+ trueFactor + ", used factor: " + usedFactor + ", " );
		if (usedFactor != 1)
			return "select div(" + xAttribute +"," + factor +"), avg(average), min(min_value), max(max_value) from "+ tableName +"pa"+ usedFactor +" where " +
				xAttribute + ">=" + (start - extent) + " and " + xAttribute + " <= " + (start + 2 * extent) + " group by div(" + xAttribute + "," +
				factor +");";
		else
			return "select div(" + xAttribute + "," + factor +"), avg(" + yAttribute + "), min(" + yAttribute + "), max(" + yAttribute+") from " +
				tableName + " where " + xAttribute + ">=" + (start - extent) +" and "+xAttribute + " <= " + (start + 2 * extent) + " group by div(" +
				xAttribute +"," + factor + ");";
	}
}
