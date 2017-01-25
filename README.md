# Assignment3.2

TasK:4package Demo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.*;

public class Task4 {
	public static void main(String[] args) throws IOException, ParseException {
	  	if (args.length != 2) {
			System.out.println("Pass two arguments");
			System.exit(1);
		}
		long modificationDate;
		String start_ts = args[0];
      String end_ts = args[1];
	//	String start_ts = "25/12/2016";
	//	String end_ts = "25/01/2017";
	Configuration conf = new Configuration();
			Path path1 = new Path("hdfs://localhost:9000/");
	      FileSystem fs = FileSystem.get(path1.toUri(),conf);
	      FileStatus[] fileStatus = fs.listStatus(path1);
	      
	      for(FileStatus status : fileStatus){
	    	  modificationDate = status.getModificationTime();
	          Date date = new Date(modificationDate);
	          SimpleDateFormat df = new SimpleDateFormat("dd/MM/yy");
	          Date start =  df.parse(start_ts);
	          Date end = df.parse(end_ts);
	    	         if ((start.compareTo(date) > 0) && (end.compareTo(date) < 0))  {
	             	  System.out.println("Directory:"+ status.getPath().toString());
	         }
	      }
	} 		      
	
	}




Task5:

package Demo;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileRead {
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("Pass one argument");
			System.exit(1);
		}

		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} 
		finally {
			IOUtils.closeStream(in);
		}
	}
}


Output:

2017-01-24 00:57:08,968 WARN  [main] util.NativeCodeLoader (NativeCodeLoader.java:<clinit>(62)) - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
10-01-1990,20
12-02-1991,22
23-12-1992,27
12-05-1993,30
10-04-1994,29
12-03-1995,25
25-02-1996,24
12-06-1997,26
10-07-1998,27
20-08-1999,28
27-09-2000,29
12-06-2001,26
10-07-2002,27
20-08-2003,28
27-09-2004,29
10-07-2005,27
20-08-2006,28
27-09-2007,29


Task6:

package Demo;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileCopy {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Pass two arguments");
			System.exit(1);
		}

		String localSrc = args[0];
		String dst = args[1];

		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst));
		
		IOUtils.copyBytes(in, out, 4096, true);
	}
}

output:

[acadgild@localhost ~]$ hadoop fs -ls /user/acadgild/hadoop/map.txt
17/01/24 00:40:07 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
-rw-r--r--   3 acadgild supergroup        253 2017-01-24 00:36 /user/acadgild/hadoop/map.txt
[acadgild@localhost ~]$ hadoop fs -cat /user/acadgild/hadoop/map.txt
17/01/24 00:40:45 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
10-01-1990,20
12-02-1991,22
23-12-1992,27
12-05-1993,30
10-04-1994,29
12-03-1995,25
25-02-1996,24
12-06-1997,26
10-07-1998,27
20-08-1999,28
27-09-2000,29
12-06-2001,26
10-07-2002,27
20-08-2003,28
27-09-2004,29
10-07-2005,27
20-08-2006,28
27-09-2007,29

