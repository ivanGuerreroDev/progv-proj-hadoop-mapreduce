package org.ivan.mapreduce.DepurarAudienciaColumn;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DepurarAudienciaMapper extends Mapper<Object, Text, Text, Text> {

    private static final Pattern PATTERN_START = Pattern.compile("^[^0-9]*(\\d+)"); // Eliminar caracteres antes de números
    // eliminar sufijos despues que terminen los numeros.
    private static final Pattern PATTERN_END = Pattern.compile("(\\d+)[^0-9]*$");
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
            // Emitir las cabeceras tal como están
            context.write(new Text(""), new Text(line));
            isFirstLine = false;
            return; // No procesar esta línea más adelante
        }
        try {
            String[] columns = csvParser.parseLine(line);                 

            if (columns != null && columns.length > 1) {
                String audiencia = columns[1].trim(); // Columna Audiencia
                audiencia = audiencia.replaceAll("\\s+", "");
                // Caso 1: Si no empieza con un número (empieza con un / o -), quitar ese carácter
                Matcher startMatcher = PATTERN_START.matcher(audiencia);
                if (startMatcher.find()) {
                    audiencia = startMatcher.group(1); // Obtener solo los números después de caracteres no numéricos
                }

                // Caso 2: eliminar esos sufijos
                Matcher endMatcher = PATTERN_END.matcher(audiencia);
                if (endMatcher.find()) {
                    audiencia = endMatcher.group(1); // Obtener solo los números antes del sufijo
                }
                
                // Caso 3: Si no es numérico, dejar vacío
                if (!audiencia.matches("^\\d+$")) {
                    audiencia = "";
                }

                // Reemplazar la columna Audiencia con el valor depurado
                columns[1] = audiencia;

                context.write(new Text(""), new Text(String.join(",", columns)));
            }
        } catch (Exception e) {
            // Manejar la excepción y loguear el error
            System.err.println("Error procesando la línea: " + line);
            e.printStackTrace();
        }
    }
}
