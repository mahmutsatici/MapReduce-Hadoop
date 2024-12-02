package org.mahmut;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; // Dosya adını almak için gerekli

public class WC_Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text word = new Text(); // Kelime
    private Text documentName = new Text(); // Doküman adı

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Şu an işlenen dosyanın adını almak için
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        documentName.set(fileName);

        // Satırı al, boşluklarla kelimelere böl
        String[] words = value.toString().split("\\s+");
        for (String w : words) {
            if (w.length() > 0) {
                word.set(w); // Kelimeyi ayarla
                context.write(word, documentName); // (kelime, doküman_adı) çıkışı yap
            }
        }
    }
}
