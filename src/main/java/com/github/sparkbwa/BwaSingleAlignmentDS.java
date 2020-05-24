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

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Class to perform the alignment over a split from the RDD of single reads
 *
 * @author José M. Abuín
 */
public class BwaSingleAlignmentDS extends BwaAlignmentBase implements MapPartitionsFunction<Row, String> {

	/**
	 * Constructor
	 * @param context The Spark context
	 * @param bwaInterpreter The BWA interpreter object to use
	 */
	public BwaSingleAlignmentDS(SparkContext context, Bwa bwaInterpreter) {
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

    	LOG.info("["+this.getClass().getName()+"] :: Tmp dir: " + this.tmpDir);
		TaskContext tc = TaskContext.get();
		int taskID = (int) tc.taskAttemptId();

		String fastqFileName1;

		if(this.tmpDir.lastIndexOf("/") == this.tmpDir.length()-1) {
			fastqFileName1 = this.tmpDir + this.appId + "-RDD" + taskID + "_1";

		}
		else {
			fastqFileName1 = this.tmpDir + "/" + this.appId + "-RDD" + taskID + "_1";

		}

		LOG.info("["+this.getClass().getName()+"] :: Writing file: " + fastqFileName1);

		File FastqFile1 = new File(fastqFileName1);
		FileOutputStream fos1;
		BufferedWriter bw1;

		ArrayList<String> returnedValues = new ArrayList<String>();

		try {
			fos1 = new FileOutputStream(FastqFile1);
			bw1 = new BufferedWriter(new OutputStreamWriter(fos1));

			Row newFastqRead = null;

			while (arg1.hasNext()) {
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
				
			}

			bw1.close();

			//We do not need the input data anymore, as it is written in a local file
			arg1 = null;

			// This is where the actual local alignment takes place
			returnedValues = this.runAlignmentProcess(taskID, fastqFileName1, null);
			

			// Delete the temporary file, as is have now been copied to the output directory
			FastqFile1.delete();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			LOG.error("["+this.getClass().getName()+"] "+e.toString());
		}

		return returnedValues.iterator();
	}
}
