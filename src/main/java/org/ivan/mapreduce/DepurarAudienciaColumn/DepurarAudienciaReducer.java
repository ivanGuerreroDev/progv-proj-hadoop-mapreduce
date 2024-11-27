package org.ivan.mapreduce.DepurarAudienciaColumn;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DepurarAudienciaReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // El Reducer simplemente toma las filas procesadas del Mapper y las escribe
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
