package org.ivan.mapreduce.RemoveFirstColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CsvJob <input path> <output path>");
            System.exit(-1);
        }
        // Configuración del job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Eliminar Primera Columna");

        // Especificar clases del Mapper y Reducer
        job.setJarByClass(Driver.class);
        job.setMapperClass(RemoveColumnMapper.class);
        job.setReducerClass(org.apache.hadoop.mapreduce.Reducer.class);

        // Establecer tipos de salida del Mapper y Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Especificar archivos de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Comprobar si el directorio de salida ya existe
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            System.err.println("El directorio de salida ya existe. Eliminando...");
            fs.delete(outputPath, true);
        }
        // Ejecutar el job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
