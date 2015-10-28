import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

//import java.util.Iterator;

public class PreAgregates /*implements Iterable<Integer>*/ {
	private int[] s;
	private int N = 0;
	
	public static final int MinimumMutiplesApproach = 1;
	
	private int[] factors;
	String xAttribute;
	String yAttribute;
	String tableName;
	double spaceLimit;
	int spacing;
	long xStart;
	long maxResolution;

	public static PreAgregates create(Connection con, int algorithm ,double spaceLimit, String xAttribute,String yAttribute,String tableName, int spacing, int maxResolution){
		int factorMax = Integer.MAX_VALUE;
		Statement st;
		String query;
		long xStart = 0;
		
		int[] factorArray;
		factorArray = new int[6];
		factorArray[0] = 1;
		factorArray[1] = 50;
		factorArray[2] = 75;
		factorArray[3] = 100;
		factorArray[4] = 300;
		factorArray[5] = 500;
		
		try{
			//Find the maximum possible factor
			st = con.createStatement();
			query = "Select Count(*) FROM "+tableName+";";
			ResultSet rs = st.executeQuery(query);
			if(rs.next())
					factorMax =(int)  rs.getInt(1)/ maxResolution;
			factorMax = 3000;
			/*int[] expectedQuerrys = new int[factorMax]; 
			for (int i=1; i<= factorMax; i++)
				expectedQuerrys[i-1] = i;
			
			int[] expectedQuerrys = new int[1];
			expectedQuerrys[0] = 2069;
			//int[] expectedQuerrys = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021, 1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163, 1171, 1181, 1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291, 1297, 1301, 1303, 1307, 1319, 1321, 1327, 1361, 1367, 1373, 1381, 1399, 1409, 1423, 1427, 1429, 1433, 1439, 1447, 1451, 1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493, 1499, 1511, 1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579, 1583, 1597, 1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657, 1663, 1667, 1669, 1693, 1697, 1699, 1709, 1721, 1723, 1733, 1741, 1747, 1753, 1759, 1777, 1783, 1787, 1789, 1801, 1811, 1823, 1831, 1847, 1861, 1867, 1871, 1873, 1877, 1879, 1889, 1901, 1907, 1913, 1931, 1933, 1949, 1951, 1973, 1979, 1987, 1993, 1997, 1999, 2069};

	
			//Find the best preagregate factors using the following algorithm
			
			factorArray = new int[factorMax];
			factorArray[0] = 1;
			switch(algorithm){
				case MinimumMutiplesApproach:
					factorArray = minimumMutiplesApproach( spaceLimit, expectedQuerrys, factorMax, factorArray, 0, Long.MAX_VALUE );
					break;
			}
			 */
			




			//Create the preagregate Tables
			for(int i = 1; i < factorArray.length; i++)
			{
	
				st = con.createStatement();
				query = "Select "+xAttribute+" FROM "+tableName+" ORDER BY "+xAttribute+" LIMIT 1";
				if(rs.next())
					xStart = rs.getLong(1);
				query = "CREATE TABLE "+tableName+factorArray[i]+"(timed bigint, average numeric(10,3), min_value numeric(10,3), max_value numeric(10,3));";
				st.executeUpdate(query);
				query = "INSERT INTO "+tableName+factorArray[i]+" SELECT div("+xAttribute+","+factorArray[i]*spacing+")*"+factorArray[i]*spacing+", avg("+yAttribute+"), min("+yAttribute+"), max("+yAttribute+") FROM "+tableName+" GROUP BY div("+xAttribute+","+factorArray[i]*spacing+");";
				st.executeUpdate(query);
			}
		}catch (SQLException e) {
			e.printStackTrace();
		} 
		
		return new PreAgregates(factorArray, spaceLimit, xAttribute, yAttribute, tableName, spacing, xStart, maxResolution);
	}

	private static int[] minimumMutiplesApproach(double spaceLimit, int[] expectedQuerrys,int factorMax, int[] factors,  int depth, long prevCost)
	{
		Integer prevFactors = null;
		long cost;


		int start;
		if (depth == 0)
			start =  (int) (1 / spaceLimit);
		else
			start = (int) (((double) factors[1])*Math.pow( ( (((double) factors[1] )*spaceLimit) / (((double) factors[1] )*spaceLimit - 1) ), depth));


		for(int i = start; i < factorMax; i++)
		{
			cost = 0;
			factors[depth+1] = i;

			for(int querryCounter = 0; querryCounter < expectedQuerrys.length; querryCounter++)
				for(int factorCounter = depth + 1; factorCounter >= 0; factorCounter--)
					if( expectedQuerrys[querryCounter] % factors[factorCounter] == 0)
					{
						cost = cost + expectedQuerrys[querryCounter] / factors[factorCounter];
						break;
					}

			if (cost < prevCost)
			{
				prevFactors =  factors[depth+1];
				prevCost =  cost;
			}

		}

		if (prevFactors == null)
		{
			int[] answer = new int[depth+1];
			for(int i = 0; i <= depth; i++)
				answer[i] = factors[i];
			return answer; 
		}
		else
		{
			factors[depth+1] = prevFactors; 
			return minimumMutiplesApproach(spaceLimit, expectedQuerrys, factorMax, factors, depth+1, prevCost);
		}
	}


	private PreAgregates(int[] factors,double spaceLimit,String xAttribute,String yAttribute,String tableName,int spacing, long xStart, long maxResolution)
	{
		this.factors = factors;
		this.spaceLimit = spaceLimit;
		this.xAttribute = xAttribute;
		this.yAttribute = yAttribute;
		this.tableName = tableName;
		this.spacing = spacing;
		this.xStart = xStart;
		this.maxResolution = maxResolution;
	}

	public String createStatement(long start, long extent)
	{
		//Calculate how many points there is in the querry

		long begining = start - extent;
		long end = start + extent*2;
		long qtdPoints;
		long factor;

		long  qi = (begining - xStart)/spacing;
		long ri = (begining - xStart)%spacing;

		long qii = (end - xStart)/spacing;
		long rii = (end - xStart)%spacing;

		if( ri!=0 )
			qtdPoints = qii - qi;
		else
			qtdPoints = qii - qi + 1;

		factor = (long) Math.floor( ((double) qtdPoints)/((double) maxResolution*3) );


		int usedFactor = 1;
		for(int i = factors.length - 1; i >= 0; i--)
		{
			if(((double)(factor%factors[i]))/((double)factor) < 0.1)
			{
				usedFactor = factors[i];
				break;
			}
		}

		System.out.print("True factor: "+factor+", "+usedFactor+";");

		if(usedFactor!=1)
			return "select div("+xAttribute+","+factor*spacing+"), avg(average),min(min_value),max(max_value) from "+tableName+usedFactor+" where "+xAttribute+">="+(start-extent)+" and "+xAttribute+" <= "+(start+2*extent)+" group by div("+xAttribute+","+factor*spacing+")";
		else
			return "select div("+xAttribute+","+factor*spacing+"), avg("+yAttribute+"),min("+yAttribute+"),max("+yAttribute+") from "+tableName+" where "+xAttribute+">="+(start-extent)+" and "+xAttribute+" <= "+(start+2*extent)+" group by div("+xAttribute+","+factor*spacing+")";
	}



























/*
	private PreAgregates()
	{	s = new int[1];	}

	private void push(int item)
	{
		if(N == s.length) resize(2*s.length);
		s[N++] = item;
	}

	private int pop()
	{
		int item = s[--N];
		if(N > 0 && N == s.length/4) resize(s.length/2);
		return item;
	}

	private void resize(int capacity)
	{
		int[] copy = new int[capacity];
		for (int i = 0; i < N; i++)
			copy[i] = s[i];
		s = copy;
	}

	public Iterator<Integer> iterator() { return new PreAgregateIterator(); }

	private class PreAgregateIterator implements Iterator<Integer>
	{
		private int i = N;

		public boolean hasNext() { return i > 0; 	}
		public void remove()	 {				 	}
		public Integer next()	 { return s[--i];	}
	}
	*/


}
