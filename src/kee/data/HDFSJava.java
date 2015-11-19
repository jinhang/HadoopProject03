package kee.data;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/* HDFS Java API
*/
public class HDFSJava {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(
				"~/hadoop-1.0.2/conf/core-site.xml"));
		conf.addResource(new Path(
				"~/hadoop-1.0.2/conf/hdfs-site.xml"));
		FileSystem fileSystem = FileSystem.get(conf);
		System.out.println(fileSystem.getUri());
		Path file = new Path("demo.txt");
		if (fileSystem.exists(file)) {
			System.out.println("File exists.");
		} else {
			
			FSDataOutputStream outStream = fileSystem.create(file);
			outStream.writeUTF("HDFS API");
			outStream.close();
		}
		FSDataInputStream inStream = fileSystem.open(file);
		String data = inStream.readUTF();
		System.out.println(data);
		inStream.close();
		fileSystem.close();
	}
}
