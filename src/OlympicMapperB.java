import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OlympicMapperB extends Mapper<Object, Text, Text, IntWritable> {

	private final IntWritable one = new IntWritable(1);
	private Text data = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
    	 
    	String line = value.toString();
    	String keyNames[] = new String[]{	"00:00 - 00:59", "01:00 - 01:59",
	    									"02:00 - 02:59", "03:00 - 03:59",
	    									"04:00 - 04:59", "05:00 - 05:59",
	    									"06:00 - 06:59", "07:00 - 07:59",
	    									"08:00 - 08:59", "09:00 - 09:59",
	    									"10:00 - 10:59", "11:00 - 11:59",
	    									"12:00 - 12:59", "13:00 - 13:59",
	    									"14:00 - 14:59", "15:00 - 15:59",
	    									"16:00 - 16:59", "17:00 - 17:59",
	    									"18:00 - 18:59", "19:00 - 19:59",
	    									"20:00 - 20:59", "21:00 - 21:59",
	    									"22:00 - 22:59", "23:00 - 23:59"};
    	
    	
    	if(line.contains(";")){
    		try {
    		String tweetParts[] = line.split(";");
    		
    			if(tweetParts.length == 4) {
	            
			    	String tweet = tweetParts[2];
					long epoch_time = Long.parseLong(tweetParts[0]);
			    	
			    	// only process tweets less or equal to 140 chars
			    	if(tweet.length()>= 1 && tweet.length()<= 140) {
			    	
			    		//Substring the hour from epoch_time
			    		String time = (LocalDateTime.ofEpochSecond(epoch_time/1000,0, ZoneOffset.ofHours(-3))).toString();
			    		String hour = time.substring(time.indexOf('T')+1);
			    		int hourPosition = Integer.parseInt(hour.substring(0,2));		
			     
			    		//Set and write the key/value
			    		data.set(keyNames[hourPosition]);
			            context.write(data,one);
	
			    	}	
    			}
    		} catch(Exception e) {}
    	}
    }
}
