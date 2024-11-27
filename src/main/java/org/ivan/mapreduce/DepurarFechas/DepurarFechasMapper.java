package org.ivan.mapreduce.DepurarFechas;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Pattern;

public class DepurarFechasMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final SimpleDateFormat TARGET_FORMAT = new SimpleDateFormat("dd/MM/yyyy");

    // Define los formatos de fecha permitidos
    private static final SimpleDateFormat[] DATE_FORMATS = new SimpleDateFormat[]{
        new SimpleDateFormat("dd/MM/yyyy"),
        new SimpleDateFormat("yyyy-MM-dd"),
        new SimpleDateFormat("dd MMM yyyy", new Locale("es")),
        new SimpleDateFormat("dd MMMM yyyy", new Locale("es")),
        new SimpleDateFormat("MMM dd, yyyy", Locale.ENGLISH),
        new SimpleDateFormat("MMMM dd, yyyy", Locale.ENGLISH)
    };

    private boolean isHeader = true; // Para controlar la primera línea (cabeceras)
    private CSVParser csvParser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Configura el parser CSV con el separador adecuado
        csvParser = new CSVParserBuilder().withSeparator(',').build();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (isHeader) {
                context.write(new Text(""), new Text(line));
                isHeader = false; // Marcar que ya pasó la primera línea
                return;
        }
        try {
            // Usar OpenCSV para leer las líneas
            String[] columns = csvParser.parseLine(line);  

            if (columns == null) return;

            // Si es la primera línea (cabecera), emítela tal cual y márcala como procesada
            

            // Asumiendo que la columna de la fecha es la primera (puedes ajustar el índice si es otra columna)
            int[] dateColumns = {2,3,54,66};
            for (int i : dateColumns) {
                if (isValidDate(columns[i])) {
                    try {
                        columns[i] = reformatDate(columns[i]); // Formatear la fecha
                    } catch (ParseException e) {
                        columns[i] = ""; // Si no se puede formatear, lo dejamos vacío
                    }
                } else {
                    columns[i] = ""; // Si no es una fecha válida, lo dejamos vacío
                }
            }
            context.write(new Text(""), new Text(String.join(",", columns)));
        } catch (Exception e) {
            // Manejar la excepción y loguear el error
            System.err.println("Error procesando la línea: " + line);
            e.printStackTrace();
        }
    }

    // Verificar si la fecha es válida en cualquiera de los formatos
    private boolean isValidDate(String dateStr) {
        for (SimpleDateFormat format : DATE_FORMATS) {
            try {
                format.setLenient(false);
                format.parse(dateStr); // Si se puede parsear, la fecha es válida
                return true;
            } catch (ParseException e) {
                // Continuar con el siguiente formato
            }
        }
        return false; // Si ninguno coincide, no es válida
    }

    // Reformatar la fecha al formato DD/MM/YYYY
    private String reformatDate(String dateStr) throws ParseException {
        for (SimpleDateFormat format : DATE_FORMATS) {
            try {
                format.setLenient(false);
                return TARGET_FORMAT.format(format.parse(dateStr)); // Formatear la fecha al formato objetivo
            } catch (ParseException e) {
                // Intentar con el siguiente formato
            }
        }
        throw new ParseException("No valid date format found", 0);
    }
}
