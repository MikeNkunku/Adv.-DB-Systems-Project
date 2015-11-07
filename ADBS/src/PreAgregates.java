import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class PreAgregates{
	private int[] s;
	private int[] factors;
	private int N = 0;
	public static final int MinimumMutiplesApproach = 1;
	String xAttribute, yAttribute, tableName;
	double spaceLimit;
	int spacing;
	long xStart, maxResolution;

	public static PreAgregates create(Connection con, String xAttribute, 
			String yAttribute, String tableName, int maxResolution) {
		int qtdPoints;
		int pointI, pointII;
		int spacing;
		Statement st;
		String query;
		long xStart;
		int[] factorArray;
		
		try {
			// Finds the number of rows in the table
			qtdPoints = numberOfTablePoints(con, tableName, maxResolution);

			//Checks if the table is clustered on the xAttribute
			clusterTable(con, tableName, xAttribute);

			//Calculate the spacing betewen two points
			spacing = calculateSpacing(con, tableName, xAttribute);

			//Look for the smallest value of xAttribute
			xStart = getXStart(con, tableName, xAttribute);

			//Delete any prior preagregates
			deletePreagregates(con, tableName);

			// Create the Preagregate Tables
			factorArray = createPreagregates(con, xAttribute, yAttribute, tableName, spacing, qtdPoints);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			return null;
		} 
		
		return new PreAgregates(factorArray, xAttribute, yAttribute, tableName, spacing, xStart, maxResolution);
	}

	private static int numberOfTablePoints (Connection con, String tableName, int maxResolution) throws Exception
	{
		int qtdPoints;
		Statement st = con.createStatement();
		String query = "SELECT COUNT(*) FROM " + tableName + ";";
		ResultSet rs = st.executeQuery(query);
		if (rs.next())
			qtdPoints = (int) (rs.getInt(1) / maxResolution);
		else
			throw new Exception("Couldn't count the number of rows of "+tableName+".\n");

		return qtdPoints;
	}

	private static void clusterTable(Connection con, String tableName, String xAttribute) throws Exception
	{
		//Checks if the table is clustered on the xAttribute
		String query = "SELECT Count(*) FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid "+
		"AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r' AND t.relname = '"+tableName+"' AND a.attname = '"+xAttribute+"';";
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(query);
		if (rs.next())
		{
			int qtdIndex = rs.getInt(1);
			if (qtdIndex == 0)
			{
				query = "CREATE INDEX "+tableName+"_xatributeIndex ON "+tableName+" ("+xAttribute+");";
				st.executeUpdate(query);
			}
		}
		else
			throw new Exception("Couldn't count the number of rows indexs of "+tableName+" on "+xAttribute+".\n");
		//Now it cluster the table on the xAtribute
		query = "CLUSTER "+tableName+" USING "+tableName+"_xatributeIndex;";
		st.executeUpdate(query);
	}

	private static int calculateSpacing(Connection con,String tableName,String xAttribute) throws Exception
	{
		//Calculate the spacing betewen two points
		int pointI, pointII;
		String query = "SELECT "+xAttribute+" FROM "+tableName+" ORDER BY "+xAttribute+" ASC LIMIT 2;";
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(query);
		if (rs.next())
			 pointI = (int) rs.getInt(1);
		else
			throw new Exception("Couldn't read the first row of "+tableName+".\n");
		if (rs.next())
			 pointII = (int) rs.getInt(1);
		else
			throw new Exception("Couldn't read the second row of "+tableName+".\n");
		return pointII - pointI;
	}

	//Look for the smallest value of xAttribute
	private static int getXStart(Connection con, String tableName, String xAttribute) throws Exception
	{
		String query = "SELECT "+xAttribute+" FROM "+tableName+" ORDER BY "+xAttribute+" ASC LIMIT 1;";
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(query);
		if (rs.next())
			return (int) rs.getInt(1);
		else
			throw new Exception("Couldn't read the first row of "+tableName+".\n");
	}

	private static void deletePreagregates(Connection con, String tableName) throws SQLException
	{		
		Statement st = con.createStatement();
		Statement st2 = con.createStatement();
		String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '"+tableName+"pa%';";
		ResultSet rs = st.executeQuery(query);		
		while (rs.next())
		{
			String prioPreagregateName = (String) rs.getString(1);
			query ="DROP TABLE "+prioPreagregateName+";";
			st2.executeUpdate(query);
		}
	}


	private static int[] createPreagregates(Connection con, String xAttribute, String yAttribute, String tableName, int spacing, int qtdPoints) throws SQLException
	{
		int counter;
		int factor;
		Statement st = con.createStatement();
		String query;
		for (factor = 20, counter = 0; factor*20 <= qtdPoints; factor = factor*2, counter++)
		{
			//To make this valid for every table it is necessary to make type independent. So it is not possible to hardcode average numeric(10,3) for example
			query = "CREATE TABLE " + tableName +"pa"+  factor + "("+xAttribute +" bigint, average numeric(10,3), min_value numeric(10,3), max_value numeric(10,3));";
			st.executeUpdate(query);
			query = "INSERT INTO " + tableName +"pa"+ factor + " SELECT div(" + xAttribute + "," + factor * spacing + ")*" +
				factor * spacing + ", avg(" + yAttribute + "), min(" + yAttribute + "), max(" + yAttribute + ") FROM " + tableName +
				" GROUP BY div("+xAttribute+","+factor*spacing+"); "+
				"CREATE INDEX "+tableName+"pa"+factor+"_xatributeIndex ON "+tableName+"pa"+factor+" ("+xAttribute+"); "+
				"CLUSTER "+tableName+"pa"+factor+" USING "+tableName+"pa"+factor+"_xatributeIndex;";
			st.executeUpdate(query);
		}

		//Here we save the factors on an array to pass to the method PreAgregates
		//There will be a problem here if the table has less than 12000 points, because counter will be zero
		int[] factorArray = new int[counter];
		for(int i = 20, ii = counter-1; i*20 <= qtdPoints; i = i*2, ii--)
			factorArray[ii] = i;

		return factorArray;
	}


	private PreAgregates(int[] factors,String xAttribute,String yAttribute,String tableName,int spacing, long xStart, long maxResolution) {
		this.factors = factors;
		this.spaceLimit = spaceLimit;
		this.xAttribute = xAttribute;
		this.yAttribute = yAttribute;
		this.tableName = tableName;
		this.spacing = spacing;
		this.xStart = xStart;
		this.maxResolution = maxResolution;
	}

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
			if(trueFactor > factors[i]*10)
			{
				usedFactor = factors[i];
				break;
			}

		return usedFactor; 
	}

	public String createStatement(long start, long extent, long factor) {
		int trueFactor = (int) factor/spacing;
		int usedFactor = bestFactor(trueFactor);

		//Generate the querry string and print what would be the true factor and the used preagregate.
		System.out.print("," + trueFactor + "," + usedFactor );
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