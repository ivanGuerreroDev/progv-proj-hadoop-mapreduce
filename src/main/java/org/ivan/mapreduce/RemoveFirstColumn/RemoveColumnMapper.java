package org.ivan.mapreduce.RemoveFirstColumn;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RemoveColumnMapper extends Mapper<Object, Text, Text, Text> {

    private CSVParser csvParser;
    private boolean isFirstLine = true;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Configura el parser CSV con el separador adecuado
        csvParser = new CSVParserBuilder().withSeparator(',').build();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (isFirstLine) {
            String[] columns = csvParser.parseLine(line);
            // Emitir las cabeceras tal como están
            StringBuilder newLine = new StringBuilder();
            for (int i = 1; i < columns.length; i++) {
                newLine.append(columns[i]);
                if (i < columns.length - 1) {
                    newLine.append(","); // Agrega la coma entre columnas
                }
            }
            context.write(new Text(""), new Text(newLine.toString()));
            isFirstLine = false;
            return; // No procesar esta línea más adelante
        }
        try {
            // Parsear la línea usando OpenCSV
            String[] columns = csvParser.parseLine(line);

            if (columns.length > 1) {
                // Reconstruir la línea sin la primera columna
                StringBuilder newLine = new StringBuilder();
                for (int i = 1; i < columns.length; i++) {
                    newLine.append(columns[i]);
                    if (i < columns.length - 1) {
                        newLine.append(","); // Agrega la coma entre columnas
                    }
                }
                // Emitir una clave vacía y el valor modificado
                context.write(new Text(""), new Text(newLine.toString()));
            }
        } catch (Exception e) {
            // Manejar la excepción y loguear el error
            System.err.println("Error procesando la línea: " + line);
            e.printStackTrace();
        }
    }
}
