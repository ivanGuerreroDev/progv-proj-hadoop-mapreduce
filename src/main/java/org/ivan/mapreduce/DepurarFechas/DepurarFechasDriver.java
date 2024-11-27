package org.ivan.mapreduce.DepurarFechas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class DepurarFechasDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: DepurarFechasDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Depurar Fechas CSV");

        job.setJarByClass(DepurarFechasDriver.class);

        // Configurar Mapper y Reducer
        job.setMapperClass(DepurarFechasMapper.class);
        job.setReducerClass(DepurarFechasReducer.class);

        // Tipos de salida del Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Tipos de salida del Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Formatos de entrada y salida
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Rutas de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Comprobar si el directorio de salida ya existe
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            System.err.println("El directorio de salida ya existe. Eliminando...");
            fs.delete(outputPath, true);
        }
        // Esperar a que el job termine
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
