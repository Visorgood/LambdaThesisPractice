import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.menthal.model.serialization.*;

import scala.reflect.ClassTag;


public class ParquetAvroConverter {
	
	public static void main(String[] args) throws IOException
	 {
		String source_dir_name = "/home/user/Documents/parquet";
		File source_dir = new File(source_dir_name);
		
		SparkContext sc = new SparkContext("local", "Parquet File Producer");
		ClassTag<SpecificRecord> ct = scala.reflect.ClassTag$.MODULE$.apply(SpecificRecord.class);
		ParquetIO$ p = ParquetIO$.MODULE$;
		
		for (File subdir : source_dir.listFiles())
		{
			for (File fileEntry : subdir.listFiles())
			{
				if (fileEntry.getName().contains("parquet"))
				{
					RDD<SpecificRecord> rdd = p.read(fileEntry.getAbsolutePath(), sc, p.read$default$3(), ct);
					
					SpecificRecord[] elems = (SpecificRecord[]) rdd.take((int) rdd.count());
					int i = 0;
					
					for (SpecificRecord elem : elems)
					{
						Schema schema = elem.getSchema();
						
						File output_file =  new File(subdir, "message" + i + ".avro");

						DatumWriter<SpecificRecord> datumWriter = new GenericDatumWriter<SpecificRecord>(schema);
						DataFileWriter<SpecificRecord> dataFileWriter = new DataFileWriter<SpecificRecord>(datumWriter);
						dataFileWriter.create(schema, output_file);
						dataFileWriter.append(elem);
						dataFileWriter.close();
						System.out.println(i);
						
						i++;
					}
				}
					
			}
	    }
	 }
}


