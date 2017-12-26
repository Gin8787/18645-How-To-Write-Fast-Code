package mapred.hashtagsim;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

public class HashtagReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> value,
						  Context context)
			throws IOException, InterruptedException {

		Map<String, Integer> counts = new HashMap<String, Integer>();

		for (Text word : value) {

            String str = word.toString();
            if(counts.containsKey(str)){
                counts.put(str, counts.get(str)+1);
            }else{
                counts.put(str, 1);
            }
		}
        ArrayList<String> tags = new ArrayList<String>(counts.keySet());
		for (int i = 0; i < tags.size(); i ++) {
            for (int j = i + 1; j < tags.size(); j ++) {
                String tag1 = tags.get(i);
                String tag2 = tags.get(j);
                Integer sum = counts.get(tag1) * counts.get(tag2);
                /**
                *remove duplicate items
                */
                if (tag1.compareTo(tag2) > 0) {
                    context.write(new Text(tag2 + "," + tag1), new IntWritable(sum));
                }       
                else if (tag1.compareTo(tag2) < 0) {
                    context.write(new Text(tag1 + "," + tag2), new IntWritable(sum));
                }   
            }
        }

	}


}
