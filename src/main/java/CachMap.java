import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class CachMap extends Mapper<LongWritable,Text,Text,NullWritable> {
    Map<String, String> pMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("product.txt"),"UTF-8"));
        String line = null;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] fields = line.split(",");
            pMap.put(fields[0], fields[1]);
        }
        reader.close();
    }

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        String pid = fields[2];

        String pname = pMap.get(pid);
        k.set(line + "\t" + pname);

        context.write(k,NullWritable.get());
    }




}
