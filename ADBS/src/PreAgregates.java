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

	public static PreAgregates create(Connection con, int algorithm, double spaceLimit, String xAttribute, 
			String yAttribute, String tableName, int spacing, int maxResolution) {
		int factorMax = Integer.MAX_VALUE;
		Statement st;
		String query;
		long xStart = 0;
		int[] factorArray = {1, 50, 75, 100, 300, 500};
		
		try {
			// Find the maximum possible factor
			st = con.createStatement();
			query = "SELECT COUNT(*) FROM " + tableName + ";";
			ResultSet rs = st.executeQuery(query);
			if (rs.next())
				factorMax = (int) (rs.getInt(1) / maxResolution);
			factorMax = 3000; // Why testing rs if factorMax will still have that value ?
			
			// Create the Preagregate Tables
			for (int i = 1; i < factorArray.length; i++) {
				st = con.createStatement();
				query = "SELECT " + xAttribute + " FROM " + tableName + " ORDER BY " + xAttribute + " LIMIT 1";
				if (rs.next())
					xStart = rs.getLong(1);
				query = "CREATE TABLE " + tableName + factorArray[i] + "(timed bigint, average numeric(10,3), min_value numeric(10,3), max_value numeric(10,3));";
				st.executeUpdate(query);
				query = "INSERT INTO " + tableName + factorArray[i] + " SELECT div(" + xAttribute + "," + factorArray[i] * spacing + ")*" +
					factorArray[i] * spacing + ", avg(" + yAttribute + "), min(" + yAttribute + "), max(" + yAttribute + ") FROM " + tableName +
					" GROUP BY div("+xAttribute+","+factorArray[i]*spacing+");";
				st.executeUpdate(query);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		
		return new PreAgregates(factorArray, spaceLimit, xAttribute, yAttribute, tableName, spacing, xStart, maxResolution);
	}

	private static int[] minimumMutiplesApproach(double spaceLimit, int[] expectedQuerrys,int factorMax, int[] factors,  int depth, long prevCost) {
		Integer prevFactors = null;
		long cost;
		int start;
		
		if (depth == 0)
			start =  (int) (1 / spaceLimit);
		else
			start = (int) (((double) factors[1])*Math.pow( ( (((double) factors[1] )*spaceLimit) / (((double) factors[1] )*spaceLimit - 1) ), depth));


		for (int i = start; i < factorMax; i++) {
			cost = 0;
			factors[depth+1] = i;

			for (int querryCounter = 0; querryCounter < expectedQuerrys.length; querryCounter++)
				for (int factorCounter = depth + 1; factorCounter >= 0; factorCounter--)
					if (expectedQuerrys[querryCounter] % factors[factorCounter] == 0) {
						cost = cost + expectedQuerrys[querryCounter] / factors[factorCounter];
						break;
					}

			if (cost < prevCost) {
				prevFactors =  factors[depth+1];
				prevCost =  cost;
			}
		}

		if (prevFactors == null) {
			int[] answer = new int[depth+1];
			for (int i = 0; i <= depth; i++)
				answer[i] = factors[i];
			return answer; 
		} else {
			factors[depth+1] = prevFactors; 
			
			return minimumMutiplesApproach(spaceLimit, expectedQuerrys, factorMax, factors, depth+1, prevCost);
		}
	}

	private PreAgregates(int[] factors,double spaceLimit,String xAttribute,String yAttribute,String tableName,int spacing, long xStart, long maxResolution) {
		this.factors = factors;
		this.spaceLimit = spaceLimit;
		this.xAttribute = xAttribute;
		this.yAttribute = yAttribute;
		this.tableName = tableName;
		this.spacing = spacing;
		this.xStart = xStart;
		this.maxResolution = maxResolution;
	}

	public String createStatement(long start, long extent) {
		// Calculate how many points there is in the query
		long begining = start - extent,
			end = start + extent*2,
			qtdPoints, factor,
			qi = (begining - xStart) / spacing,
			ri = (begining - xStart) % spacing,
			qii = (end - xStart) / spacing,
			rii = (end - xStart) % spacing;

		if (ri != 0)
			qtdPoints = qii - qi;
		else
			qtdPoints = qii - qi + 1;

		factor = (long) Math.floor(((double) qtdPoints)/((double) maxResolution*3) );

		int usedFactor = 1;
		for (int i = factors.length - 1; i >= 0; i--) {
			if (((double)(factor%factors[i]))/((double)factor) < 0.1) {
				usedFactor = factors[i];
				break;
			}
		}

		System.out.print("True factor: " + factor + ", " + usedFactor + ";");

		if (usedFactor != 1)
			return "select div(" + xAttribute +"," + factor * spacing +"), avg(average), min(min_value), max(max_value) from "+ tableName + usedFactor +" where " +
				xAttribute + ">=" + (start - extent) + " and " + xAttribute + " <= " + (start + 2 * extent) + " group by div(" + xAttribute + "," +
				factor * spacing+")";
		else
			return "select div(" + xAttribute + "," + factor * spacing +"), avg(" + yAttribute + "), min(" + yAttribute + "), max(" + yAttribute+") from " +
				tableName + " where " + xAttribute + ">=" + (start - extent) +" and "+xAttribute + " <= " + (start + 2 * extent) + " group by div(" +
				xAttribute +"," + factor * spacing + ")";
	}
}