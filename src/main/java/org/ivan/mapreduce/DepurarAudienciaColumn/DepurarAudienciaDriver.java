package org.ivan.mapreduce.DepurarAudienciaColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DepurarAudienciaDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DepurarAudienciaDriver <input path> <output path>");
            System.exit(-1);
        }

        // Configuración del trabajo de Hadoop
        Configuration conf = new Configuration();

        // Crear el trabajo de MapReduce
        Job job = Job.getInstance(conf, "Depurar Audiencia");
        job.setJarByClass(DepurarAudienciaDriver.class);

        // Establecer el Mapper y Reducer
        job.setMapperClass(DepurarAudienciaMapper.class);
        job.setReducerClass(DepurarAudienciaReducer.class);

        // Establecer las clases de salida (tipos de clave y valor)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Establecer las clases de entrada y salida
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Configurar el directorio de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Comprobar si el directorio de salida ya existe
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            System.err.println("El directorio de salida ya existe. Eliminando...");
            fs.delete(outputPath, true);
        }

        // Ejecutar el trabajo de MapReduce
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
