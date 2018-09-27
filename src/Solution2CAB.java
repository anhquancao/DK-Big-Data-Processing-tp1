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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.Parser.Token;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Solution2CAB {
	public static class CountItemsMapper extends Mapper<Object, Text, Text, MapWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] items = value.toString().trim().split(" ");

			for (int i = 0; i < items.length; i++) {
				String item1 = items[i];
				HashMap<String, Integer> map = new HashMap<>();
				for (int j = 0; j < items.length; j++) {
					String item2 = items[j];
					if (!item1.equalsIgnoreCase(item2)) {
						Integer occs = map.get(item2);
						if (occs == null) {
							map.put(item2, 1);
						} else {
							map.put(item2, occs + 1);
						}
					}
				}

				MapWritable mapWritable = new MapWritable();
				for (String keyStr : map.keySet()) {
					Text keyText = new Text(keyStr);
					int occs = map.get(keyStr);
					mapWritable.put(keyText, new IntWritable(occs));
				}
				Text outputText = new Text(item1);
				context.write(outputText, mapWritable);
			}
		}
	}

	public static class ProductCountsCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {

		public void reduce(Text key, Iterable<MapWritable> maps, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<>();

			for (MapWritable mapItem : maps) {

				for (Writable mapKey : mapItem.keySet()) {
					Text textItem = (Text) mapKey;
					String item = textItem.toString();

					int occ = Integer.parseInt(mapItem.get(textItem).toString());

					Object storedOcc = map.get(item);
					if (storedOcc == null) {
						map.put(item, occ);
					} else {
						int nOcc = (int) storedOcc;
						map.put(item, occ + nOcc);
					}
				}

			}

			MapWritable outputMap = new MapWritable();
			for (String keyStr : map.keySet()) {
				int valueInt = (int) map.get(keyStr);
				outputMap.put(new Text(keyStr), new IntWritable(valueInt));
			}

			context.write(key, outputMap);
		}
	}

	public static class ProductCountsReducer extends Reducer<Text, MapWritable, Text, Text> {

		public void reduce(Text key, Iterable<MapWritable> maps, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<>();

			for (MapWritable mapItem : maps) {

				for (Writable mapKey : mapItem.keySet()) {
					Text textItem = (Text) mapKey;
					String item = textItem.toString();

					int occ = Integer.parseInt(mapItem.get(textItem).toString());

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

			// Sort to get the most items that is commonly bought with
			Collections.sort(list, new Comparator<Entry<String, Integer>>() {
				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					Entry<String, Integer> entry1 = (Entry<String, Integer>) o1;
					Entry<String, Integer> entry2 = (Entry<String, Integer>) o2;
					return entry2.getValue() - entry1.getValue();
				}
			});

			// Output the map of all items
			// Left is the most common, and the right is the least
			// Output all the bought with items as string below.
			// Example: (cd12,3) (dvd13,2) (book12,1) (book32,1)
			// Left is the most common, and the right is the least
			
			String res = "";
			for (Iterator it = list.iterator(); it.hasNext();) {
				Map.Entry entry = (Map.Entry) it.next();
				res = res + "(" + entry.getKey() + ", " + entry.getValue() + ") ";
			}
			context.write(key, new Text(res));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Customer Also Bought Solution 2");
		job.setJarByClass(Solution2CAB.class);
		job.setMapperClass(CountItemsMapper.class);
		job.setCombinerClass(ProductCountsCombiner.class);
		job.setReducerClass(ProductCountsReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
