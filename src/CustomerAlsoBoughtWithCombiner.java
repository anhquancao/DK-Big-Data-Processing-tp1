import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.Parser.Token;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerAlsoBoughtWithCombiner {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Text mapOutputKey = new Text();
			Text mapOutputValue = new Text();
			String[] items = value.toString().trim().split(" ");
			for (int i = 0; i < items.length; i++) {
				for (int j = i; j < items.length; j++) {
					String item1 = items[i];
					String item2 = items[j];
					if (!item1.equalsIgnoreCase(item2)) {
						mapOutputKey.set(item1);
						mapOutputValue.set(item2);
						context.write(mapOutputKey, mapOutputValue);

						mapOutputKey.set(item2);
						mapOutputValue.set(item1);
						context.write(mapOutputKey, mapOutputValue);

					}
				}
			}
		}
	}

	public static class ProductCountCombiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text result = new Text();
			HashMap<String, Integer> map = new HashMap<>();

			for (Text val : values) {
				String sVal = val.toString();
				Object occ = map.get(sVal);
				if (occ == null) {
					map.put(sVal, 1);
				} else {
					int nOcc = (int) occ;
					map.put(sVal, nOcc + 1);
				}
			}

			String res = "";
			Iterator it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				res += pair.getKey() + "," + pair.getValue() + " ";
			}

			result.set(res);
			context.write(key, result);
		}
	}

	public static class ProductCountsReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text result = new Text();
			HashMap<String, Integer> map = new HashMap<>();

			for (Text val : values) {
				String[] listVal = val.toString().trim().split(" ");
				for (String s : listVal) {
					String[] temp = s.split(",");
					String item = temp[0];
					int occ = Integer.parseInt(temp[1]);

					Object storedOcc = map.get(item);
					if (storedOcc == null) {
						map.put(item, occ);
					} else {
						int nOcc = (int) storedOcc;
						map.put(item, occ + nOcc);
					}
				}

			}

			List<Entry<String, Integer>> list = new LinkedList<>(map.entrySet());

			Collections.sort(list, new Comparator<Entry<String, Integer>>() {
				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					Entry<String, Integer> entry1 = (Entry<String, Integer>) o1;
					Entry<String, Integer> entry2 = (Entry<String, Integer>) o2;
					return entry2.getValue() - entry1.getValue();
				}
			});

			String res = "";

			for (Iterator it = list.iterator(); it.hasNext();) {
				Map.Entry entry = (Map.Entry) it.next();
				res = res + "(" + entry.getKey() + ", " + entry.getValue() + ") ";
			}

			result.set(res);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Cusomter Also Bought 1");
		job.setJarByClass(CustomerAlsoBoughtWithCombiner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(ProductCountCombiner.class);
		job.setReducerClass(ProductCountsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
