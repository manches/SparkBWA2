/**
 * Copyright 2016 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of SparkBWA.
 *
 * <p>SparkBWA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>SparkBWA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with SparkBWA. If not,
 * see <http://www.gnu.org/licenses/>.
 */
package com.github.sparkbwa;

//import org.apache.hadoop.io.Text;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Row;

import scala.Tuple2;

import java.nio.file.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 * Class to perform the alignment over a split from the RDD of paired reads
 *
 * @author José M. Abuín
 */
public class BwaPairedAlignmentDS extends BwaAlignmentBase implements MapPartitionsFunction<Row, String> {

	/**
	 * Constructor
	 * @param context The Spark context
	 * @param bwaInterpreter The BWA interpreter object to use
	 */
	public BwaPairedAlignmentDS(SparkContext context, Bwa bwaInterpreter) {
		super(context, bwaInterpreter);
  }

	/**
	 * Code to run in each one of the mappers. This is, the alignment with the corresponding entry
	 * data The entry data has to be written into the local filesystem
	 * @param arg0 The RDD Id
	 * @param arg1 An iterator containing the values in this RDD
	 * @return An iterator containing the sam file name generated
	 * @throws Exception
	 */
	public Iterator<String> call(Iterator<Row> arg0) throws Exception {

		// STEP 1: Input fastq reads tmp file creation
		LOG.error("["+this.getClass().getName()+"] :: Tmp dir: " + this.tmpDir);
		TaskContext tc = TaskContext.get();
		int taskID = (int) tc.taskAttemptId();
		LOG.error("["+this.getClass().getName()+"] :: TaskID: " + taskID);

		String fastqFileName1;
		String fastqFileName2;

		if(this.tmpDir.lastIndexOf("/") == this.tmpDir.length()-1) {
			fastqFileName1 = this.tmpDir + this.appId + "-DF" + taskID + "_1";
			fastqFileName2 = this.tmpDir + this.appId + "-DF" + taskID + "_2";
		}
		else {
			fastqFileName1 = this.tmpDir + "/" + this.appId + "-DF" + taskID + "_1";
			fastqFileName2 = this.tmpDir + "/" + this.appId + "-DF" + taskID + "_2";
		}


		LOG.error("["+this.getClass().getName()+"] :: Writing file: " + fastqFileName1);
		LOG.error("["+this.getClass().getName()+"] :: Writing file: " + fastqFileName2);

		File FastqFile1 = new File(fastqFileName1);
		File FastqFile2 = new File(fastqFileName2);

		String group = "usc";
		UserPrincipalLookupService lookupService = FileSystems.getDefault()
		            .getUserPrincipalLookupService();
		GroupPrincipal group = lookupService.lookupPrincipalByGroupName(group);
		Files.getFileAttributeView(FastqFile1, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).setGroup(group);
		Files.getFileAttributeView(FastqFile2, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).setGroup(group);
		
		

		FileOutputStream fos1;
		FileOutputStream fos2;
		StringWriter sw1 = null;
		StringWriter sw2 = null;
		BufferedWriter bw1;
		BufferedWriter bw2;

		ArrayList<String> returnedValues = new ArrayList<String>();

		//We write the data contained in this split into the two tmp files
		try {
			fos1 = new FileOutputStream(FastqFile1);
			fos2 = new FileOutputStream(FastqFile2);

			bw1 = new BufferedWriter(new OutputStreamWriter(fos1));
			bw2 = new BufferedWriter(new OutputStreamWriter(fos2));

			Row newFastqRead = null;

			while (arg0.hasNext()) {
				
				newFastqRead = arg0.next();
				

				//LOG.error("["+this.getClass().getName()+"] :: newFastqRead: identifier1 " + newFastqRead.<String>getAs("identifier1"));
				//LOG.error("["+this.getClass().getName()+"] :: newFastqRead: sequence1 " + newFastqRead.<String>getAs("sequence1"));
				//LOG.error("["+this.getClass().getName()+"] :: newFastqRead: aux1 " + newFastqRead.<String>getAs("aux1"));
				//LOG.error("["+this.getClass().getName()+"] :: newFastqRead: quality1 " + newFastqRead.<String>getAs("quality1"));
				//LOG.error("["+this.getClass().getName()+"] :: newFastqRead: identifier2 " + newFastqRead.<String>getAs("identifier2"));
				//LOG.error("["+this.getClass().getName()+"] :: newFastqRead: sequence2 " + newFastqRead.<String>getAs("sequence2"));
				//LOG.error("["+this.getClass().getName()+"] :: newFastqRead: aux2 " + newFastqRead.<String>getAs("aux2"));
				//LOG.error("["+this.getClass().getName()+"] :: newFastqRead: quality2 " + newFastqRead.<String>getAs("quality2"));

				bw1.write(newFastqRead.<String>getAs("identifier1"));
				bw1.newLine();
				bw1.write(newFastqRead.<String>getAs("sequence1"));
				bw1.newLine();
				bw1.write(newFastqRead.<String>getAs("aux1"));
				bw1.newLine();
				bw1.write(newFastqRead.<String>getAs("quality1"));
				bw1.newLine();
				
				bw2.write(newFastqRead.<String>getAs("identifier2"));
				bw2.newLine();
				bw2.write(newFastqRead.<String>getAs("sequence2"));
				bw2.newLine();
				bw2.write(newFastqRead.<String>getAs("aux2"));
				bw2.newLine();
				bw2.write(newFastqRead.<String>getAs("quality2"));
				bw2.newLine();
				
			}

	        bw1.close();
			bw2.close();

			arg0 = null;

			// This is where the actual local alignment takes place
			returnedValues = this.runAlignmentProcess(taskID, fastqFileName1, fastqFileName2);

			// Delete temporary files, as they have now been copied to the output directory
			LOG.error("["+this.getClass().getName()+"] :: Deleting file: " + fastqFileName1);
			//FastqFile1.delete();

			LOG.error("["+this.getClass().getName()+"] :: Deleting file: " + fastqFileName2);
			//FastqFile2.delete();
	        

	        
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			LOG.error("["+this.getClass().getName()+"] "+e.toString());
		}

		return returnedValues.iterator();
	}
}
