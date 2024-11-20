package org.ivan.mapreduce.RemoveFirstColumn;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RemoveColumnMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Leer la línea del CSV
        String line = value.toString();

        // Separar la línea por comas
        String[] fields = line.split(",");

        // Eliminar la primera columna y concatenar el resto
        if (fields.length > 1) {
            StringBuilder newLine = new StringBuilder();
            for (int i = 1; i < fields.length; i++) {
                newLine.append(fields[i]);
                if (i < fields.length - 1) {
                    newLine.append(",");
                }
            }

            // Emitir la nueva línea sin la primera columna
            context.write(new Text(newLine.toString()), new Text(""));
        }
    }
}
