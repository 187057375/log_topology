package logparser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class pvLogParser {
	
//	private static final Pattern LINE_PATTERN = Pattern.compile(
//		  "(\\S+:)?(\\S+? \\S+?) \\S+? DEBUG \\S+? - DEMANDE_ID=(\\d+?) - listener (\\S+?) : (\\S+?)");
	
	private static final Pattern LINE_PATTERN = Pattern.compile(
			  ".+\"GET \\/\\d\\.gif\\?(\\S+) HTTP\\/\\d\\.\\d\" \\d+ \\d+ \"(.+)\" \"(.*)\" \"(.*)\" \\w+ \"(.*)\"");
	//regex = re.compile(r".+\"GET /\d\.gif\?([^\s]+) HTTP\/\d\.\d\" \d+ \d+ \"(.+)\" \"(.*)\" \"(.*)\" \w+ \"(.*)\"")
	
	//private static final String DATE_PATTERN = null;

	public static Map<String,String> parse(String line) throws ParseException {
		
			Map<String,String> map=new HashMap<String,String>();
			
//		    String demandId;
//		    String listenerClass;
//		    long startTime;
//		    long endTime;
			String query = "";

		    //SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
		    Matcher matcher = LINE_PATTERN.matcher(line);
		    if (matcher.matches()) {
//		        int offset = matcher.groupCount()-4; // 4 interesting groups, the first is optional
//		        String demandeId = matcher.group(2+offset);
//		        listenerClass = matcher.group(3+offset);
//		        long time = sdf.parse(matcher.group(1+offset)).getTime();
//		        if ("starting".equals(matcher.group(4+offset))) {
//		            startTime = time;
//		            endTime = -1;
//		        } else {
//		            startTime = -1;
//		            endTime = time;
//		        }
		    	query = matcher.group(1); 
		    }
	    	map.put("query", query);
	        return map;
		}

}







