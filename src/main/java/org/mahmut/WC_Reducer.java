package org.mahmut;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WC_Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> documentFrequency = new HashMap<>(); // Doküman ve frekansları saklamak için

        // Her kelimenin doküman_id'sini topla ve say
        for (Text docId : values) {
            String doc = docId.toString();
            documentFrequency.put(doc, documentFrequency.getOrDefault(doc, 0) + 1);
        }

        // Çıktı formatı: "kelime -> doküman_adı1(frekans), doküman_adı2(frekans), ..."
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Integer> entry : documentFrequency.entrySet()) {
            result.append(entry.getKey()).append("(").append(entry.getValue()).append("), ");
        }

        // Son virgülü kaldır
        if (result.length() > 0) {
            result.setLength(result.length() - 2);
        }

        // Sonucu (kelime, dokümanlar ve frekanslar) olarak yaz
        context.write(key, new Text(result.toString()));
    }
}

