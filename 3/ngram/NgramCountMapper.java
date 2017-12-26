package mapred.ngramcount;

import java.io.IOException;
import java.util.*;//Siqiao
import mapred.util.Tokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	int n_gram;
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);

//		for (String word : words)
//			context.write(new Text(word), NullWritable.get());


		//Siqiao
		for( int i = 0 ; i < words.length - n_gram + 1; ++i){
			String keystr = words[i];
			for (int j = i + 1; j < i + n_gram; j ++){
				keystr = keystr + ' ' + words[j];
			}
			context.write(new Text(keystr), NullWritable.get());
		}


	}


	//Siqiao
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		String str_n_gram = context.getConfiguration().get("num_gram");
		n_gram = Integer.parseInt(str_n_gram);

	}


}
