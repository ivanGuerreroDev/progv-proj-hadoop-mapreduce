package org.ivan.mapreduce.RemoveFirstColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
    public static void main(String[] args) throws Exception {
        // Configuración del job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Eliminar Primera Columna");

        // Especificar clases del Mapper y Reducer
        job.setJarByClass(Driver.class);
        job.setMapperClass(RemoveColumnMapper.class);
        job.setReducerClass(RemoveColumnReducer.class);

        // Establecer tipos de salida del Mapper y Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Especificar archivos de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Ejecutar el job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
