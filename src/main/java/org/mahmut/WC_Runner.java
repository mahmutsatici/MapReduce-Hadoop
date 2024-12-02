package org.mahmut;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mahmut.WC_Mapper;
import org.mahmut.WC_Reducer;

public class WC_Runner {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count Inverted Index");

        job.setJarByClass(WC_Runner.class);
        job.setMapperClass(WC_Mapper.class);
        job.setReducerClass(WC_Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Giriş ve çıkış yollarını ayarla
        FileInputFormat.addInputPath(job, new Path(args[0])); // Giriş yolu (input klasörü)
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Çıkış yolu (output klasörü)

        // İşin başarıyla tamamlanıp tamamlanmadığını kontrol et
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
