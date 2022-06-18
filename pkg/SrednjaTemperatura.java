package pkg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SrednjaTemperatura {

    public static class AvgTuple implements Writable {

        private int minTemp = 0;
        private int minCounter = 0;
        private int maxTemp = 0;
        private int maxCounter = 0;

        @Override
        public void readFields(DataInput in) throws IOException { //citanje csv fajlova
            minTemp = in.readInt();
            minCounter = in.readInt();
            maxTemp = in.readInt();
            maxCounter = in.readInt();
        }
        @Override
        public void write(DataOutput out) throws IOException { //ispisivanje
            out.writeInt(minTemp);
            out.writeInt(minCounter);
            out.writeInt(maxTemp);
            out.writeInt(maxCounter);
        }

        public String toString() {
            return "MinAverage: " + (1.0 * minTemp/minCounter) + ", MaxAverage: " + (1.0 * maxTemp/maxCounter);
        
        }
        
        public int getMinTemp() {
            return minTemp;
        }

        public void setMinTemp(int minTemp) {
            this.minTemp = minTemp;
        }

        public int getMinCounter() {
            return minCounter;
        }

        public void setMinCounter(int minCounter) {
            this.minCounter = minCounter;
        }

        public int getMaxTemp() {
            return maxTemp;
        }

        public void setMaxTemp(int maxTemp) {
            this.maxTemp = maxTemp;
        }

        public int getMaxCounter() {
            return maxCounter;
        }

        public void setMaxCounter(int maxCounter) {
            this.maxCounter = maxCounter;
        }
        
    }


    public static class TempMapper extends Mapper<Object, Text, Text, AvgTuple> {
        private Text month = new Text();
        private AvgTuple outTuple = new AvgTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            month.set(line[1].substring(4,6));
            int temperature = Integer.parseInt(line[3]);

            //TMAX or TMIN
            String extreme = line[2];
            if(extreme.equals("TMIN")){
                outTuple.setMinTemp(temperature);
                outTuple.setMinCounter(1);
            }else if(extreme.equals("TMAX")){
                outTuple.setMaxTemp(temperature);
                outTuple.setMaxCounter(1);
            }

            context.write(month, outTuple);
        }
    }

    public static class TempReducer extends Reducer<Text, AvgTuple, Text, AvgTuple> {

        private AvgTuple resultTuple = new AvgTuple();

        public void reduce(Text key, Iterable<AvgTuple> tuples, Context context) throws IOException, InterruptedException {
            int minTemp = 0;
            int maxTemp = 0;
            int minCounter = 0;
            int maxCounter = 0;

            for(AvgTuple tup : tuples){
                minTemp += tup.getMinTemp();
                maxTemp += tup.getMaxTemp();
                minCounter += tup.getMinCounter();
                maxCounter += tup.getMaxCounter();
            }

            resultTuple.setMinTemp(minTemp);
            resultTuple.setMinCounter(minCounter);
            resultTuple.setMaxTemp(maxTemp);
            resultTuple.setMaxCounter(maxCounter);

            context.write(key, resultTuple);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average extreme temperature");
        job.setJarByClass(SrednjaTemperatura.class);
        job.setMapperClass(TempMapper.class);
        job.setCombinerClass(TempReducer.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AvgTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,  new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}