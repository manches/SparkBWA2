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

import java.util.Arrays;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ContextCleaner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.mllib.rdd.RDDFunctions;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * BwaInterpreter class
 *
 * @author Jose M. Abuin
 * @brief This class communicates Spark with BWA
 */
public class BwaInterpreter {

	private static final Log 				LOG = LogFactory.getLog(BwaInterpreter.class); // The LOG
	private SparkConf 						sparkConf; 								// The Spark Configuration to use
	private SparkSession 					sparkSession; 								// The Spark Session to use
	private SQLContext						sqlContext;
	private JavaSparkContext 				ctx;									// The Java Spark Context
	private Configuration 					conf;									// Global Configuration
	private JavaRDD<Tuple2<String, String>> dataRDD;
	private long 							totalInputLength;
	private long 							blocksize;
	private BwaOptions 						options;								// Options for BWA
	private String 							inputTmpFileName;


	/**
	 * Constructor to build the BwaInterpreter object from the Spark shell When creating a
	 * BwaInterpreter object from the Spark shell, the BwaOptions and the Spark Context objects need
	 * to be passed as argument.
	 *
	 * @param opcions The BwaOptions object initialized with the user options
	 * @param context The Spark Context from the Spark Shell. Usually "sc"
	 * @return The BwaInterpreter object with its options initialized.
	 */
	public BwaInterpreter(BwaOptions optionsFromShell, SparkContext context) {

		this.options = optionsFromShell;
		this.ctx = new JavaSparkContext(context);
		this.initInterpreter();
	}

	/**
	 * Constructor to build the BwaInterpreter object from within SparkBWA
	 *
	 * @param args Arguments got from Linux console when launching SparkBWA with Spark
	 * @return The BwaInterpreter object with its options initialized.
	 */
	public BwaInterpreter(String[] args) {

		this.options = new BwaOptions(args);
		this.initInterpreter();
	}

	/**
	 * Method to get the length from the FASTQ input or inputs. It is set in the class variable totalInputLength
	 */
	private void setTotalInputLength() {
		try {
			// Get the FileSystem
			FileSystem fs = FileSystem.get(this.conf);

			// To get the input files sizes
			ContentSummary cSummaryFile1 = fs.getContentSummary(new Path(options.getInputPath()));

			long lengthFile1 = cSummaryFile1.getLength();
			long lengthFile2 = 0;

			if (!options.getInputPath2().isEmpty()) {
				ContentSummary cSummaryFile2 = fs.getContentSummary(new Path(options.getInputPath()));
				lengthFile2 = cSummaryFile2.getLength();
			}

			// Total size. Depends on paired or single reads
			this.totalInputLength = lengthFile1 + lengthFile2;
			fs.close();
		} catch (IOException e) {
			LOG.error(e.toString());
			e.printStackTrace();
		}
	}

	/**
	 * Method to create the output folder in HDFS
	 */
	private void createOutputFolder() {
		try {
			FileSystem fs = FileSystem.get(this.conf);

			// Path variable
			Path outputDir = new Path(options.getOutputPath());

			// Directory creation
			if (!fs.exists(outputDir)) {
				fs.mkdirs(outputDir);
			}
			else {
				fs.delete(outputDir, true);
				fs.mkdirs(outputDir);
			}

			fs.close();
		}
		catch (IOException e) {
			LOG.error(e.toString());
			e.printStackTrace();
		}
	}

	/**
	 * Function to load a FASTQ file from HDFS into a JavaPairRDD<Long, String>
	 * @param ctx The JavaSparkContext to use
	 * @param pathToFastq The path to the FASTQ file
	 * @return A JavaPairRDD containing <Long Read ID, String Read>
	 */
	public static JavaPairRDD<Long, String> loadFastq(JavaSparkContext ctx, String pathToFastq) {
		JavaRDD<String> fastqLines = ctx.textFile(pathToFastq);

		// Determine which FASTQ record the line belongs to.
		JavaPairRDD<Long, Tuple2<String, Long>> fastqLinesByRecordNum = fastqLines.zipWithIndex().mapToPair(new FASTQRecordGrouper());

		// Group group the lines which belongs to the same record, and concatinate them into a record.
		return fastqLinesByRecordNum.groupByKey().mapValues(new FASTQRecordCreator());
	}
	



	/**
	 * Method to perform and handle the single reads sorting
	 * @return A RDD containing the strings with the sorted reads from the FASTQ file
	 */
	private JavaRDD<String> handleSingleReadsSorting() {
		JavaRDD<String> readsRDD = null;

		long startTime = System.nanoTime();

		LOG.info("["+this.getClass().getName()+"] :: Not sorting in HDFS. Timing: " + startTime);

		// Read the FASTQ file from HDFS using the FastqInputFormat class
		JavaPairRDD<Long, String> singleReadsKeyVal = loadFastq(this.ctx, this.options.getInputPath());

		// Sort in memory with no partitioning
		if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
			// First, the join operation is performed. After that,
			// a sortByKey. The resulting values are obtained
			readsRDD = singleReadsKeyVal.sortByKey().values();
			LOG.info("["+this.getClass().getName()+"] :: Sorting in memory without partitioning");
		}

		// Sort in memory with partitioning
		else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
			singleReadsKeyVal = singleReadsKeyVal.repartition(options.getPartitionNumber());
			readsRDD = singleReadsKeyVal.sortByKey().values();//.persist(StorageLevel.MEMORY_ONLY());
			LOG.info("["+this.getClass().getName()+"] :: Repartition with sort");
		}

		// No Sort with no partitioning
		else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
			LOG.info("["+this.getClass().getName()+"] :: No sort and no partitioning");
			readsRDD = singleReadsKeyVal.values();
		}

		// No Sort with partitioning
		else {
			LOG.info("["+this.getClass().getName()+"] :: No sort with partitioning");
			int numPartitions = singleReadsKeyVal.partitions().size();

			/*
			 * As in previous cases, the coalesce operation is not suitable
			 * if we want to achieve the maximum speedup, so, repartition
			 * is used.
			 */
			if ((numPartitions) <= options.getPartitionNumber()) {
				LOG.info("["+this.getClass().getName()+"] :: Repartition with no sort");
			}
			else {
				LOG.info("["+this.getClass().getName()+"] :: Repartition(Coalesce) with no sort");
			}

			readsRDD = singleReadsKeyVal
				.repartition(options.getPartitionNumber())
				.values();
				//.persist(StorageLevel.MEMORY_ONLY());

		}

		long endTime = System.nanoTime();
		LOG.info("["+this.getClass().getName()+"] :: End of sorting. Timing: " + endTime);
		LOG.info("["+this.getClass().getName()+"] :: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");

		//readsRDD.persist(StorageLevel.MEMORY_ONLY());

		return readsRDD;
	}

	/**
	 * Method to perform and handle the paired reads sorting
	 * @return A JavaRDD containing grouped reads from the paired FASTQ files
	 */
//	private Dataset handlePairedReadsSorting() {
	private JavaRDD<Tuple2<String, String>> handlePairedReadsSorting() {
		JavaRDD<Tuple2<String, String>> readsRDD = null;
		Dataset df1 = null;
		Dataset df2 = null;
		Dataset df = null;
		Dataset dfFinal = null;
		long startTime = System.nanoTime();

		LOG.info("["+this.getClass().getName()+"] ::Not sorting in HDFS. Timing: " + startTime);

		// Read the two FASTQ files from HDFS using the loadFastq method. After that, a Spark join operation is performed
		JavaPairRDD<Long, String> datasetTmp1 = loadFastq(this.ctx, options.getInputPath());
		JavaPairRDD<Long, String> datasetTmp2 = loadFastq(this.ctx, options.getInputPath2());
		JavaPairRDD<Long, Tuple2<String, String>> pairedReadsRDD = datasetTmp1.join(datasetTmp2);
		
		//dfFinal = this.sparkSession.createDataset(this.ctx.textFile(options.getInputPath()).sliding(4))
		//		.toDF("identifier", "sequence", "quality");
		JavaRDD<String> rAUX1 = this.ctx.textFile(options.getInputPath());
		RDD<Object,Object,Object,Object> r1  = RDDFunctions.fromRDD(rAUX1.rdd(), rAUX1.classTag())
				.sliding(4,4);
		
		ArrayList<String> result = new ArrayList<>();


		
		
		//JavaRDD<String> x = JavaRDD.fromRDD(r1, r1.classTag()); 
		//dfFinal = this.sparkSession.createDataset( rAUX1.rdd().sliding(4,4)).toDF();
		/*
		//Read the input file and store as Row RDD
		JavaRDD<Row> dataRDD = r1.map( line -> {
										String[] parts = line.split(" ");
														
										return RowFactory.create(parts[0],parts[1],parts[1],parts[3]);
					  				 }
							   );
		
		
		//Define the schema of the data  
		StructType schema = new StructType( new StructField[] 
							{
								new StructField("identifier", DataTypes.StringType, false, Metadata.empty()),
								new StructField("sequence", DataTypes.StringType, false, Metadata.empty()),
								new StructField("aux", DataTypes.StringType, false, Metadata.empty()),
								new StructField("quality", DataTypes.StringType, false, Metadata.empty())
							}
						  );
		
		//Create a DataSet using data and schema
		Dataset<Row> df = spark.createDataFrame(dataRDD, schema);
		
		//use this statement when only required for testing.
		df.show();
		*/
		
		/*
		 LOG.info("[ ] :: -------------------------------------------: "  + result.getClass() );
		result.foreach(rdd -> {
			LOG.info("[ ] :: MANCHES result RDD - handlePairedReadsSorting : " + rdd);
		LOG.info("[ ] :: -------------------------------------------: ");
		});
		 */
		
		
		
				//.map {
		 // case Array(id, seq, _, qual) -> (id, seq, qual)
	//	}).toDF("identifier", "sequence", "quality");

		//RDD<Object> r2 = RDDFunctions.fromRDD(pairedReadsRDD.rdd(), pairedReadsRDD.classTag()).sliding(4,4);
		//RDD<Object> r = RDDFunctions.fromRDD(pairedReadsRDD.rdd(), pairedReadsRDD.classTag()).sliding(7);
		//df = sparkSession.createDataset(JavaPairRDD.toRDD(pairedReadsRDD),Encoders.tuple(Encoders.LONG(), Encoders.tuple(Encoders.STRING(),Encoders.STRING()) )  ).toDF();
		
		//Encoder<Tuple2<Long, Tuple2<String,String>>> encoderf =
		//Encoders.tuple(Encoders.LONG(), Encoders.tuple(Encoders.STRING(),Encoders.STRING()));
		//Dataset<Row> userViolationsDetails = spark.createDataset(JavaPairRDD.toRDD(MY_RDD),encoder2).toDF("value1","value2");
		//df = this.sparkSession.createDataset(JavaPairRDD.toRDD(pairedReadsRDD),encoderf).toDF();
		
		Encoder<Tuple2<Long, String>> encoder1 =
		Encoders.tuple(Encoders.LONG(), Encoders.STRING());
		//Dataset<Row> userViolationsDetails = spark.createDataset(JavaPairRDD.toRDD(MY_RDD),encoder2).toDF("value1","value2");
		df1 = this.sparkSession.createDataset(JavaPairRDD.toRDD(datasetTmp1),encoder1).toDF();

		Encoder<Tuple2<Long, String>> encoder2 =
		Encoders.tuple(Encoders.LONG(), Encoders.STRING());
		//Dataset<Row> userViolationsDetails = spark.createDataset(JavaPairRDD.toRDD(MY_RDD),encoder2).toDF("value1","value2");
		df2 = this.sparkSession.createDataset(JavaPairRDD.toRDD(datasetTmp2),encoder2).toDF();
		
				df1.show(1,false);
				df1.printSchema();
				df2.show(1,false);
				df2.printSchema();
		
		df = df1.join(df2,df1.col("_2").equalTo(df2.col("_2")));		
		df.show(1000,false);
		df.printSchema();

						
		datasetTmp1.unpersist();
		datasetTmp2.unpersist();
		//pairedReadsRDD.unpersist();
		LOG.info("[ ] :: -------------------------------------------: ");
		pairedReadsRDD.foreach(rdd -> {
		LOG.info("[ ] :: MANCHES ANTES - handlePairedReadsSorting : " + rdd);
		LOG.info("[ ] :: -------------------------------------------: ");
		});

		// Sort in memory with no partitioning
		if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
			readsRDD = pairedReadsRDD.sortByKey().values();
			//dfFinal = df.orderBy("_1").select("_2");
			LOG.info("["+this.getClass().getName()+"] :: Sorting in memory without partitioning");
		}

		// Sort in memory with partitioning
		else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
			pairedReadsRDD = pairedReadsRDD.repartition(options.getPartitionNumber());
			readsRDD = pairedReadsRDD.sortByKey().values();//.persist(StorageLevel.MEMORY_ONLY());
			
			//Dataset dfAux = df.repartition(options.getPartitionNumber());
			//dfFinal = dfAux.orderBy("_1").select("_2");
			
			LOG.info("["+this.getClass().getName()+"] :: Repartition with sort");
		}

		// No Sort with no partitioning
		else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
			LOG.info("["+this.getClass().getName()+"] :: No sort and no partitioning");
		}

		// No Sort with partitioning
		else {
			LOG.info("["+this.getClass().getName()+"] :: No sort with partitioning");
			int numPartitions = pairedReadsRDD.partitions().size();

			/*
			 * As in previous cases, the coalesce operation is not suitable
			 * if we want to achieve the maximum speedup, so, repartition
			 * is used.
			 */
			if ((numPartitions) <= options.getPartitionNumber()) {
				LOG.info("["+this.getClass().getName()+"] :: Repartition with no sort");
			}
			else {
				LOG.info("["+this.getClass().getName()+"] :: Repartition(Coalesce) with no sort");
			}
			
			readsRDD = pairedReadsRDD
				.repartition(options.getPartitionNumber())
				.values();
				//.persist(StorageLevel.MEMORY_ONLY());
			
			//dfFinal = df.repartition(options.getPartitionNumber()).select("_2");
		}

		long endTime = System.nanoTime();

		LOG.info("["+this.getClass().getName()+"] :: End of sorting. Timing: " + endTime);
		LOG.info("["+this.getClass().getName()+"] :: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");
		//readsRDD.persist(StorageLevel.MEMORY_ONLY());
		
		LOG.info("[ ] :: -------------------------------------------: ");
			readsRDD.foreach(rdd -> {
			LOG.info("[ ] :: MANCHES FINAL - handlePairedReadsSorting : " + rdd);
			LOG.info("[ ] :: -------------------------------------------: ");

	    });
		//Dataset dfF = dfFinal.select("_2");
		//dfF.show(1,false);
		//dfF.printSchema();
	    
		 
		//return dfFinal;
		System.exit(1);
		return readsRDD;
		
		
		
	}

	/**
	 * Procedure to perform the alignment using paired reads
	 * @param bwa The Bwa object to use
	 * @param readsRDD The RDD containing the paired reads
	 * @return A list of strings containing the resulting sam files where the output alignments are stored
	 */
	//private List<String> MapPairedBwa(Bwa bwa, Dataset readsDS) {
	private List<String> MapPairedBwa(Bwa bwa, JavaRDD<Tuple2<String, String>> readsRDD) {
		// The mapPartitionsWithIndex is used over this RDD to perform the alignment. The resulting sam filenames are returned
		/*readsRDD.foreach(rdd -> {
			LOG.info("[ ] :: MANCHESFINAL: " + rdd);
			LOG.info("[ ] :: -------------------------------------------: ");
	    });
	   
		LOG.info("[ ] :: MANCHES 1 -------------------------------------------: ");
		readsDS.show(1,false);
		readsDS.printSchema();
		JavaRDD readsRDD = null;
		LOG.info("[ ] :: 	 -------------------------------------------: ");
		LOG.info("[ ] :: " + readsDS.count());
		LOG.info("[ ] :: MANCHES 3.1 -------------------------------------------: ");
		LOG.info("[ ] :: " + Arrays.toString(readsDS.dtypes()) );
		LOG.info("[ ] :: MANCHES 3 -------------------------------------------: ");
		readsRDD = readsDS.javaRDD();
		LOG.info("[ ] :: MANCHES 4.1 -------------------------------------------: ");
		LOG.info("[ ] :: " + readsRDD.count() );

		LOG.info("[ ] :: MANCHES 4 -------------------------------------------: ");
		readsRDD.foreach(rdd -> {
			LOG.info("[ ] :: MANCHES - MapPairedBwa : " + rdd);
			LOG.info("[ ] :: -------------------------------------------: ");

	    });
		LOG.info("[ ] :: MANCHES 5 -------------------------------------------: ");
		
		return readsRDD
			.mapPartitionsWithIndex(new BwaPairedAlignment(readsRDD.context(), bwa), true)
			.collect();
		
		
		SparkContext contextAux = readsDS.sparkSession().sparkContext()
		
		return readsDS
				.javaRDD()
				.mapPartitions(new BwaPairedAlignment(contextAux, bwa), true)
				.collect();
		*/
		
		return readsRDD
				.mapPartitionsWithIndex(new BwaPairedAlignment(readsRDD.context(), bwa), true)
				.collect();
	}

	/**
	 *
	 * @param bwa The Bwa object to use
	 * @param readsRDD The RDD containing the paired reads
	 * @return A list of strings containing the resulting sam files where the output alignments are stored
	 */
	private List<String> MapSingleBwa(Bwa bwa, JavaRDD<String> readsRDD) {
		// The mapPartitionsWithIndex is used over this RDD to perform the alignment. The resulting sam filenames are returned
		return readsRDD
			.mapPartitionsWithIndex(new BwaSingleAlignment(readsRDD.context(), bwa), true)
			.collect();
	}

  /**
   * Runs BWA with the specified options
   *
   * @brief This function runs BWA with the input data selected and with the options also selected
   *     by the user.
   */
	public void runBwa() {
		LOG.info("["+this.getClass().getName()+"] :: Starting BWA");
		Bwa bwa = new Bwa(this.options);

		List<String> returnedValues;
		if (bwa.isPairedReads()) {
			//Dataset readsDS = handlePairedReadsSorting();
			JavaRDD<Tuple2<String, String>> readsRDD = handlePairedReadsSorting();
			returnedValues = MapPairedBwa(bwa, readsRDD);
			
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			returnedValues.forEach(System.out::println);
			System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");


		}
		else {
			JavaRDD<String> readsRDD = handleSingleReadsSorting();
			returnedValues = MapSingleBwa(bwa, readsRDD);
			
		}

		// In the case of use a reducer the final output has to be stored in just one file
		if(this.options.getUseReducer()) {
			try {
				FileSystem fs = FileSystem.get(this.conf);

				Path finalHdfsOutputFile = new Path(this.options.getOutputHdfsDir() + "/FullOutput.sam");
				FSDataOutputStream outputFinalStream = fs.create(finalHdfsOutputFile, true);

				// We iterate over the resulting files in HDFS and agregate them into only one file.
				for (int i = 0; i < returnedValues.size(); i++) {
					LOG.info("JMAbuin:: SparkBWA :: Returned file ::" + returnedValues.get(i));
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(returnedValues.get(i)))));

					String line;
					line = br.readLine();

					while (line != null) {
						if (i == 0 || !line.startsWith("@")) {
							//outputFinalStream.writeBytes(line+"\n");
							outputFinalStream.write((line + "\n").getBytes());
						}

						line = br.readLine();
					}
					br.close();

					fs.delete(new Path(returnedValues.get(i)), true);
				}

				outputFinalStream.close();
				fs.close();
			} catch (IOException e) {
				e.printStackTrace();
				LOG.error(e.toString());
			}
		}
		/* // Previous version doesn't makes sense. We do not have tmp files in HDFS
		for (String outputFile : returnedValues) {
			LOG.info("["+this.getClass().getName()+"] :: SparkBWA:: Returned file ::" + outputFile);
			//After the execution, if the inputTmp exists, it should be deleted
			try {
				if ((this.inputTmpFileName != null) && (!this.inputTmpFileName.isEmpty())) {
					FileSystem fs = FileSystem.get(this.conf);
					fs.delete(new Path(this.inputTmpFileName), true);
					fs.close();
				}
			}
			catch (IOException e) {
				e.printStackTrace();
				LOG.error(e.toString());
			}
		}
		*/
	}

	/**
	 * Procedure to init the BwaInterpreter configuration parameters
	 */
	public void initInterpreter() {
		//If ctx is null, this procedure is being called from the Linux console with Spark
		if (this.ctx == null) {

			String sorting;

			//Check for the options to perform the sort reads
			if (options.isSortFastqReads()) {
				sorting = "SortSpark";
			}
			else if (options.isSortFastqReadsHdfs()) {
				sorting = "SortHDFS";
			}
			else {
				sorting = "NoSort";
			}

			//The application name is set
			this.sparkConf = new SparkConf().setAppName("SparkBWA_"
					+ options.getInputPath().split("/")[options.getInputPath().split("/").length - 1]
					+ "-"
					+ options.getPartitionNumber()
					+ "-"
					+ sorting);

			//The ctx is created from scratch
			this.ctx = new JavaSparkContext(this.sparkConf);

			this.sparkSession = SparkSession
					.builder()
					.config(this.sparkConf)
					.getOrCreate();
			
			sqlContext = new SQLContext(this.ctx);
			
			
		}
		//Otherwise, the procedure is being called from the Spark shell
		else {

			this.sparkConf = this.ctx.getConf();
		}

		//The Hadoop configuration is obtained
		this.conf = this.ctx.hadoopConfiguration();

		//The block size
		this.blocksize = this.conf.getLong("dfs.blocksize", 134217728);

		createOutputFolder();
		setTotalInputLength();

		//ContextCleaner cleaner = this.ctx.sc().cleaner().get();
	}
}