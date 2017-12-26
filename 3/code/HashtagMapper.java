package mapred.hashtagsim;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;

public class HashtagMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();	
		String[] words = Tokenizer.tokenize(line);
		/*
		 * Iterate all words, find out all hashtags, then iterate all other non-hashtag 
		 * words and map out.
		 */
		for (String word : words){
			if (word.startsWith("#")) {
				for (String word2 : words){
					if (word2.startsWith("#")==false){
						// context.write(new Text(word), new Text(word2));
						/*
						* here we exchange the position of original key and value
						* so that we could compute for each word, how many time it
						* connects with a certain hashtag
						*/
						
						context.write(new Text(word2), new Text(word));

					}
				}
			}
		}
		
	}
}
