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
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.mllib.rdd.RDDFunctions;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.*;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.io.IOUtils;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.FileNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.File;
import org.apache.spark.mllib.rdd.*;
import org.apache.spark.mllib.rdd.RDDFunctions;


import org.apache.spark.api.java.function.Function;
import java.util.AbstractCollection;
import java.io.Serializable;
import com.google.common.collect.Lists;

import com.google.common.collect.ImmutableList; 
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.spark.sql.expressions.Window;
import java.util.HashMap; 
import java.util.Map;
import org.apache.spark.sql.expressions.UserDefinedFunction;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
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
	
	
	public static Dataset<Row> zipWithIndex(Dataset<Row> df, Long offset, String indexName) {
        Dataset<Row> dfWithPartitionId = df
                .withColumn("partition_id", spark_partition_id())
                .withColumn("inc_id", monotonically_increasing_id());

        Object partitionOffsetsObject = dfWithPartitionId
                .groupBy("partition_id")
                .agg(count(lit(1)).alias("cnt"), first("inc_id").alias("inc_id"))
                .orderBy("partition_id")
                .select(col("partition_id"), sum("cnt").over(Window.orderBy("partition_id")).minus(col("cnt")).minus(col("inc_id")).plus(lit(offset).alias("cnt")))
                .collect();
        Row[] partitionOffsetsArray = ((Row[]) partitionOffsetsObject);
        Map<Integer, Long> partitionOffsets = new HashMap<>();
        for (int i = 0; i < partitionOffsetsArray.length; i++) {
            partitionOffsets.put(partitionOffsetsArray[i].getInt(0), partitionOffsetsArray[i].getLong(1));
        }

        UserDefinedFunction getPartitionOffset = udf(
                (partitionId) -> partitionOffsets.get((Integer) partitionId), DataTypes.LongType
        );

        return dfWithPartitionId
                .withColumn("partition_offset", getPartitionOffset.apply(col("partition_id")))
                .withColumn(indexName, col("partition_offset").plus(col("inc_id")))
                .drop("partition_id", "partition_offset", "inc_id");
    }
	
	
	/**
	 * Function to load a FASTQ file from HDFS into a JavaPairRDD<Long, String>
	 * @param ctx The JavaSparkContext to use
	 * @param pathToFastq The path to the FASTQ file
	 * @return A JavaPairRDD containing <Long Read ID, String Read>
	 */
	public static JavaPairRDD<Long, String> loadFastq(SparkSession ss, String pathToFastq) {

		JavaSparkContext ctx = JavaSparkContext.fromSparkContext(ss.sparkContext());
		
		JavaRDD<String> fastqLines = ctx.textFile(pathToFastq);

		
	    StructField field2 = DataTypes.createStructField("identifier", DataTypes.StringType, true);
	    StructField field3 = DataTypes.createStructField("sequence", DataTypes.StringType, true);
	    StructField field4 = DataTypes.createStructField("aux", DataTypes.StringType, true);
	    StructField field5 = DataTypes.createStructField("quality", DataTypes.StringType, true);
	    StructType schema = DataTypes.createStructType(Lists.newArrayList( field2, field3, field4, field5));
	    
 
 
		   JavaRDD<String> filteredJavaRDD = ctx.textFile(pathToFastq).filter(new
						   Function<String,Boolean>(){
						   public Boolean call(String arg0) throws Exception {
						   return (!arg0.equals(""));
						   }
						   });	  		   
		   
		   JavaRDD<Row> cRDD = filteredJavaRDD
				   .map((Function<String, Row>) record -> {
			      String[] parts = record.split("\n");
			      //return RowFactory.create(attributes[0], attributes[1].trim());
		    	  //System.out.println("filteredJavaRDD********** |||"+parts[0].trim()+"|||||||");
			      //if (parts[0].trim().startsWith("@")) {
			    	//  System.out.println("YES");

			    	//  return RowFactory.create(parts[0].trim(),parts[1].trim(),parts[2].trim(),parts[3].trim());  
			      //} else {
			    	//  System.out.println("NO");

			    	  return RowFactory.create("@"+parts[0].trim(),parts[1].trim(),parts[2].trim(),parts[3].trim());
			    //  }
			     
			      
			      
			      
			    });
		      // collect RDD for printing
	        for(String line:filteredJavaRDD.collect()){
	            System.out.println("filteredJavaRDD********** "+line);
	        }
	        	
      Dataset<Row> mainDataset = ss.createDataFrame(cRDD, schema).withColumn("index1", functions.monotonicallyIncreasingId());     
      mainDataset.show(10,false);
      
//		Encoder<Tuple2<Long, Tuple2<String,Long>>> encoder2 =
//		Encoders.tuple(Encoders.LONG(), Encoders.tuple(Encoders.STRING(),Encoders.LONG()));
//Dataset<Row> newDataSet = this. .createDataset(JavaPairRDD.toRDD(fastqLinesByRecordNum),encoder2).toDF("value1","value2");

//		newDataSet.printSchema();


// pipe character | is the record seperator

		//JavaRDD<String> fastqLines = ctx.textFile(pathToFastq);

	   
/*	   JavaRDD<Row> cRDD = ctx.textFile(pathToFastq)
             .map(new Function<String, row>() {
                    @Override
                    public row call(String line) throws Exception {
                           String[] parts = line.split("\n");
                           Row r = null;
                           r = RowFactory.create(parts[0].trim(),parts[1].trim(),parts[2].trim(),parts[3].trim());
                           return r;
                    }
             });
*/	  	   
		   

		// Determine which FASTQ record the line belongs to.
		JavaPairRDD<Long, Tuple2<String, Long>> fastqLinesByRecordNum = fastqLines.zipWithIndex().mapToPair(new FASTQRecordGrouper());

		// Group group the lines which belongs to the same record, and concatinate them into a record.
		return fastqLinesByRecordNum.groupByKey().mapValues(new FASTQRecordCreator());
	}
	
	
	
	
	
	/**
	 * Function to load a FASTQ file from HDFS into a JavaPairRDD<Long, String>
	 * @param ctx The JavaSparkContext to use
	 * @param pathToFastq The path to the FASTQ file
	 * @return A JavaPairRDD containing <Long Read ID, String Read>
	 */	
	public static JavaPairRDD<Long, String> loadFastqold(SparkSession ss, String pathToFastq) {
		JavaSparkContext ctx = JavaSparkContext.fromSparkContext(ss.sparkContext());
		JavaRDD<String> fastqLines = ctx.textFile(pathToFastq);

		// Determine which FASTQ record the line belongs to.
		JavaPairRDD<Long, Tuple2<String, Long>> fastqLinesByRecordNum = fastqLines.zipWithIndex().mapToPair(new FASTQRecordGrouper());
		
		// Group group the lines which belongs to the same record, and concatinate them into a record.
		return fastqLinesByRecordNum.groupByKey().mapValues(new FASTQRecordCreator());
	}
	
	/**
	 * Function to load a FASTQ file from HDFS into a JavaPairRDD<Long, String>
	 * @param ctx The JavaSparkContext to use
	 * @param pathToFastq The path to the FASTQ file
	 * @return A JavaPairRDD containing <Long Read ID, String Read>

	//public static JavaPairRDD<Long, String> loadFastqtoDS(JavaSparkContext ctx, String pathToFastq) {
	public static Dataset<Row> loadFastqtoDS3(SQLContext sc, String pathToFastq, int index) {

			
	    StructField field2 = DataTypes.createStructField("identifier"+index, DataTypes.StringType, true);
	    StructField field3 = DataTypes.createStructField("sequence"+index, DataTypes.StringType, true);
	    StructField field4 = DataTypes.createStructField("aux"+index, DataTypes.StringType, true);
	    StructField field5 = DataTypes.createStructField("quality"+index, DataTypes.StringType, true);
	    StructType schema = DataTypes.createStructType(Lists.newArrayList(field2, field3, field4, field5));

	    //Dataset<Row> data = sqlContext.createDataFrame(rowList, schema);	
			
		JavaRDD<String> fastqLines = sc.textFile(pathToFastq);		
		Dataset<Row> rowList = sc.createDataset(sc.textFile(pathToFastq).sliding(4, 4).map()
		  case Array(id, seq, aux, qual) => (id, seq, aux, qual)
		),schema);
		Dataset<Row> years = file8Data.map((MapFunction<Row, Integer>) row -> row.<Integer>getAs("YEAR"), Encoders.INT());

				
	    //DataFrame df = sqlContext.createDataFrame(rowRDD, schema);
		return rowList.zipWithIndex(rowList,1,"index");

		//LOG.error("[ ] :: -------------------------------------------: ");
		//fastqLines.zipWithIndex().foreach(rdd -> {
		//LOG.error("[ ] :: MANCHES FINAL - loadFastq : " + rdd);
		//LOG.error("[ ] :: -------------------------------------------: ");

    //});
		
		
		/*
		 
	public static Dataset<Row> zipWithIndex(Dataset<Row> df, String name) {
    JavaRDD<Row> rdd = df.javaRDD().zipWithIndex().map(t -> {
        Row r = t._1;
        Long index = t._2 + 1;
        ArrayList<Object> list = new ArrayList<>();
        r.toSeq().iterator().foreach(x -> list.add(x));
        list.add(index);
        return RowFactory.create(list);
    });
    StructType newSchema = df.schema()
            .add(new StructField(name, DataTypes.LongType, true, null));
    return df.sparkSession().createDataFrame(rdd, newSchema);
}

		 
		
		}
	*/
	
	/**
	 * Function to load a FASTQ file from HDFS into a JavaPairRDD<Long, String>
	 * @param ctx The JavaSparkContext to use
	 * @param pathToFastq The path to the FASTQ file
	 * @return A JavaPairRDD containing <Long Read ID, String Read>
	 */
	public static Dataset<Row> loadFastqtoDSnew(SparkSession ss, String pathToFastq, int index) {			


	    StructField field2 = DataTypes.createStructField("identifier"+index, DataTypes.StringType, true);
	    StructField field3 = DataTypes.createStructField("sequence"+index, DataTypes.StringType, true);
	    StructField field4 = DataTypes.createStructField("aux"+index, DataTypes.StringType, true);
	    StructField field5 = DataTypes.createStructField("quality"+index, DataTypes.StringType, true);
	    StructType schema = DataTypes.createStructType(Lists.newArrayList( field2, field3, field4, field5));

		JavaSparkContext ctx = JavaSparkContext.fromSparkContext(ss.sparkContext());
		JavaRDD<String> fastqLines = ctx.textFile(pathToFastq);
		
		RDD<Object> rf =  RDDFunctions.fromRDD(fastqLines.rdd(),fastqLines.classTag()).sliding(4, 4);
		JavaRDD<Object> x = new JavaRDD<>(rf, rf.elementClassTag());
		
		
		//Map the object RDD to String RDD
		JavaRDD<Row> result = x.map(new Function<Object,Row>() {
			@Override
			public Row call(Object arg0) throws Exception {
				  return RowFactory.create(Object[])arg0);
//				  String text = Arrays.toString((Object[])arg0);
//			      String[] parts = text.substring(1, text.length()-1).split(",");
//
//		    	return RowFactory.create(parts[0].trim(),parts[1].trim(),parts[2].trim(),parts[3].trim());

			}
		});	     
        Dataset<Row> mainDataset = zipWithIndex(ss.createDataFrame(result, schema),1L,"index");     

		//	Dataset<Row> mainDataset = ss.createDataFrame(result, schema).withColumn("index", functions.monotonicallyIncreasingId());     
//			mainDataset.show(10,false);
  
		return mainDataset;
	}

	
		
	/**
	 * Function to load a FASTQ file from HDFS into a JavaPairRDD<Long, String>
	 * @param ctx The JavaSparkContext to use
	 * @param pathToFastq The path to the FASTQ file
	 * @return A JavaPairRDD containing <Long Read ID, String Read>
	 */
	public static Dataset<Row> loadFastqtoDS(SparkSession ss, String pathToFastq, int index) {			


	    StructField field2 = DataTypes.createStructField("identifier"+index, DataTypes.StringType, true);
	    StructField field3 = DataTypes.createStructField("sequence"+index, DataTypes.StringType, true);
	    StructField field4 = DataTypes.createStructField("aux"+index, DataTypes.StringType, true);
	    StructField field5 = DataTypes.createStructField("quality"+index, DataTypes.StringType, true);
	    StructType schema = DataTypes.createStructType(Lists.newArrayList( field2, field3, field4, field5));

		JavaSparkContext ctx = JavaSparkContext.fromSparkContext(ss.sparkContext());
		System.out.println("AQUI");
		   
		JavaRDD<String> fastqLines = ctx.textFile(pathToFastq);
		
    	//List<String> data = Arrays.asList(""); 
    	//JavaRDD<String> items = ctx.parallelize(data,1);
    	
		//JavaRDD<String> filteredJavaRDD = items.union(fastqLines);
		
        //for(String line:filteredJavaRDD.collect()){
        //    System.out.println("filteredJavaRDD********** "+line);
       //}
		  /*JavaRDD<String> filteredJavaRDD = filteredJavaRDD2.filter(new
		  *Function<String,Boolean>(){ public Boolean call(String arg0) throws Exception
		  *{ return (!arg0.equals("")); } });
		  */
		   
	
			System.out.println("AQUI2");
			

		    JavaRDD<Row> cRDD = fastqLines
		    		.map((Function<String, Row>) record -> {try
		    		{
		    			String[] parts = record.split("\n");
		    			//return RowFactory.create(attributes[0], attributes[1].trim());
		    			if ( parts[0].trim().startsWith("@") ) {
			    			return RowFactory.create(parts[0].trim(),parts[1].trim(),parts[2].trim(),parts[3].trim());		    				
		    			}else {		    				
			    			return RowFactory.create("@"+parts[0].trim(),parts[1].trim(),parts[2].trim(),parts[3].trim());
		    			}
	    } catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
			LOG.error("[MANCHESSSSSSSSS]     "+ record+"||||||||"+e.toString());
		} 
	                return null;

		    		});

		    

			System.out.println("AQUI3");

		Dataset<Row> mainDataset = ss.createDataFrame(cRDD, schema).withColumn("index", functions.monotonicallyIncreasingId());     
		System.out.println("AQUI4");
		mainDataset.show(10,false);
        
  
        //Dataset<Row> data = sqlContext.createDataFrame(rowList, schema);
        //data.show(false);

		return mainDataset;
	}


	/**
	 * Method to perform and handle the single reads sorting
	 * @return A RDD containing the strings with the sorted reads from the FASTQ file
	 */
	private JavaRDD<String> handleSingleReadsSorting() {
		JavaRDD<String> readsRDD = null;

		long startTime = System.nanoTime();

		LOG.error("["+this.getClass().getName()+"] :: Not sorting in HDFS. Timing: " + startTime);

		// Read the FASTQ file from HDFS using the FastqInputFormat class
		JavaPairRDD<Long, String> singleReadsKeyVal = loadFastq(this.sparkSession, this.options.getInputPath());

		// Sort in memory with no partitioning
		if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
			// First, the join operation is performed. After that,
			// a sortByKey. The resulting values are obtained
			readsRDD = singleReadsKeyVal.sortByKey().values();
			LOG.error("["+this.getClass().getName()+"] :: Sorting in memory without partitioning");
		}

		// Sort in memory with partitioning
		else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
			singleReadsKeyVal = singleReadsKeyVal.repartition(options.getPartitionNumber());
			readsRDD = singleReadsKeyVal.sortByKey().values();//.persist(StorageLevel.MEMORY_ONLY());
			LOG.error("["+this.getClass().getName()+"] :: Repartition with sort");
		}

		// No Sort with no partitioning
		else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
			LOG.error("["+this.getClass().getName()+"] :: No sort and no partitioning");
			readsRDD = singleReadsKeyVal.values();
		}

		// No Sort with partitioning
		else {
			LOG.error("["+this.getClass().getName()+"] :: No sort with partitioning");
			int numPartitions = singleReadsKeyVal.partitions().size();

			/*
			 * As in previous cases, the coalesce operation is not suitable
			 * if we want to achieve the maximum speedup, so, repartition
			 * is used.
			 */
			if ((numPartitions) <= options.getPartitionNumber()) {
				LOG.error("["+this.getClass().getName()+"] :: Repartition with no sort"+"["+numPartitions+"]");
			}
			else {
				LOG.error("["+this.getClass().getName()+"] :: Repartition(Coalesce) with no sort"+"["+numPartitions+"]");
			}

			readsRDD = singleReadsKeyVal
				.repartition(options.getPartitionNumber())
				.values();
				//.persist(StorageLevel.MEMORY_ONLY());

		}

		long endTime = System.nanoTime();
		LOG.error("["+this.getClass().getName()+"] :: End of sorting. Timing: " + endTime);
		LOG.error("["+this.getClass().getName()+"] :: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");

		//readsRDD.persist(StorageLevel.MEMORY_ONLY());

		return readsRDD;
	}

	/**
	 * Method to perform and handle the paired reads sorting
	 * @return A JavaRDD containing grouped reads from the paired FASTQ files
	 */
	private Dataset<Row> handlePairedReadsSorting() {
//	private JavaRDD<Tuple2<String, String>> handlePairedReadsSorting() {
		JavaRDD<Tuple2<String, String>> readsRDD = null;
		Dataset<Row> dfFinal = null;
		long startTime = System.nanoTime();

		LOG.error("["+this.getClass().getName()+"] ::Not sorting in HDFS. Timing: " + startTime);

		// Read the two FASTQ files from HDFS using the loadFastq method. After that, a Spark join operation is performed
//		JavaPairRDD<Long, String> datasetTmp1 = loadFastq(this.sparkSession, options.getInputPath());
//		JavaPairRDD<Long, String> datasetTmp2 = loadFastq(this.sparkSession, options.getInputPath2());
//		JavaPairRDD<Long, Tuple2<String, String>> pairedReadsRDD = datasetTmp1.join(datasetTmp2);
		Dataset<Row> datasettmpDS1 = loadFastqtoDSnew(this.sparkSession, options.getInputPath(),1);
		LOG.error("["+this.getClass().getName()+"] ::Not sorting in HDFS. datasettmpDS1: " );

		Dataset<Row> datasettmpDS2 = loadFastqtoDSnew(this.sparkSession, options.getInputPath2(),2);
		LOG.error("["+this.getClass().getName()+"] ::Not sorting in HDFS. datasettmpDS2");
		
		datasettmpDS1.show(10,false);
		datasettmpDS1.count();
		System.out.println("AQQQQQQQQUi " + datasettmpDS1.count());

		
		datasettmpDS2.show(10,false);
		datasettmpDS2.count();
		System.out.println("AQQQQQQQQUi " + datasettmpDS2.count());


		Dataset<Row> joined = datasettmpDS1.join(datasettmpDS2,"index");
		LOG.error("["+this.getClass().getName()+"] ::Not sorting in HDFS. joined ");
		
		joined.orderBy("index").show(10,false);		
		joined.count();		
		//datasetTmp1.unpersist();
		//datasetTmp2.unpersist();
		
		// Sort in memory with no partitioning
		if ((options.getPartitionNumber() == 0) && (options.isSortFastqReads())) {
			//readsRDD = pairedReadsRDD.sortByKey().values();
			dfFinal = joined.orderBy("index");
			LOG.error("["+this.getClass().getName()+"] :: Sorting in memory without partitioning");
		}

		// Sort in memory with partitioning
		else if ((options.getPartitionNumber() != 0) && (options.isSortFastqReads())) {
			//pairedReadsRDD = pairedReadsRDD.repartition(options.getPartitionNumber());
			//readsRDD = pairedReadsRDD.sortByKey().values();//.persist(StorageLevel.MEMORY_ONLY());
			
			Dataset<Row> dfAux = joined.repartition(options.getPartitionNumber());
			dfFinal = joined.orderBy("index");
			
			LOG.error("["+this.getClass().getName()+"] :: Repartition with sort");
		}

		// No Sort with no partitioning
		else if ((options.getPartitionNumber() == 0) && (!options.isSortFastqReads())) {
			LOG.error("["+this.getClass().getName()+"] :: No sort and no partitioning");
		}

		// No Sort with partitioning
		else {
			LOG.error("["+this.getClass().getName()+"] :: No sort with partitioning");
			//int numPartitions = pairedReadsRDD.partitions().size();  
			int numPartitions = joined.rdd().getNumPartitions();   
			/*
			 * As in previous cases, the coalesce operation is not suitable 
			 * if we want to achieve the maximum speedup, so, repartition
			 * is used.
			 */
			if ((numPartitions) <= options.getPartitionNumber()) {
				LOG.error("["+this.getClass().getName()+"] :: Repartition with no sort"+"["+numPartitions+"]"+"["+options.getPartitionNumber()+"]");
			}
			else {
				LOG.error("["+this.getClass().getName()+"] :: Repartition(Coalesce) with no sort"+"["+numPartitions+"]"+"["+options.getPartitionNumber()+"]");
			}
			
			//readsRDD = pairedReadsRDD
			//	.repartition(options.getPartitionNumber())
			//	.values();
				//NO ES EL CASO -> .persist(StorageLevel.MEMORY_ONLY());
			
			dfFinal = joined.repartition(options.getPartitionNumber());
		}

		long endTime = System.nanoTime();

		LOG.error("["+this.getClass().getName()+"] :: End of sorting. Timing: " + endTime);
		LOG.error("["+this.getClass().getName()+"] :: Total time: " + (endTime - startTime) / 1e9 / 60.0 + " minutes");
		//readsRDD.persist(StorageLevel.MEMORY_ONLY());
		
		//LOG.error("[ ] :: -------------------------------------------: ");
		//	readsRDD.foreach(rdd -> {
		//	LOG.error("[ ] :: MANCHES FINAL - handlePairedReadsSorting : " + rdd);
		//	LOG.error("[ ] :: -------------------------------------------: ");
		// });
/*
 * 
 * 
 19/08/17 11:21:18 INFO sparkbwa.BwaInterpreter: [ ] :: MANCHES FINAL - handlePairedReadsSorting : 
(
	@rotavirus_128_641_2:0:0_1:0:0_563
	TATTTTCATTTACTTTCTTCGCATTTTGTACCCAAGTATTGTATTTCTCATTAACCCTCTGTTTTAGTGA
	+
	2222222222222222222222222222222222222222222222222222222222222222222222
,
	@rotavirus_128_641_2:0:0_1:0:0_563
	ATTCAATATGATTACAATGCAGTATTTACCAGAGTTAAAAGTAAAATTGATTATGTGATGGATGACTCTG
	+
	2222222222222222222222222222222222222222222222222222222222222222222222
)

 * 
 * 
 * 
 * 
 */
	
 		dfFinal.show(10,false);
		dfFinal.printSchema();
		System.out.println("AQQQQQQQQUi " + dfFinal.count());
	    
		/*root
		 |-- index: integer (nullable = true)
		 |-- identifier1: string (nullable = true)
		 |-- sequence1: string (nullable = true)
		 |-- aux1: string (nullable = true)
		 |-- quality1: string (nullable = true)
		 |-- identifier2: string (nullable = true)
		 |-- sequence2: string (nullable = true) 
		 |-- aux2: string (nullable = true)
		 |-- quality2: string (nullable = true)
		 
		 index			|1295 
		 identifier1	|@rotavirus_352_829_2:0:0_1:0:0_50e 
		 sequence1		|TGAATTTAACTGCTGTTCAAAGTCATTCTTTACATCATCTGGTAATTCCATAGACCTTAGATACCACTCA
		 aux1			|+   
		 quality1		|2222222222222222222222222222222222222222222222222222222222222222222222
		 identifier2	|@rotavirus_352_829_2:0:0_1:0:0_50e 
		 sequence2		|TTCTAAAGGGATCGACCAAAAGATGAGAGTACTTAATGCATGTTTTAGTGTAAAAAGAATACCAGGAACA
		 aux2			|+   
		 quality2		|2222222222222222222222222222222222222222222222222222222222222222222222
		 
		 *
		 */
		 
		return dfFinal;
		//return readsRDD;
		
		
		
	}

	/**
	 * Procedure to perform the alignment using paired reads
	 * @param bwa The Bwa object to use
	 * @param readsRDD The RDD containing the paired reads
	 * @return A list of strings containing the resulting sam files where the output alignments are stored
	 */
	private List<String> MapPairedBwa(Bwa bwa, Dataset<Row> readsDS) {
	//private List<String> MapPairedBwa(Bwa bwa, JavaRDD<Tuple2<String, String>> readsRDD) {
		// The mapPartitionsWithIndex is used over this RDD to perform the alignment. The resulting sam filenames are returned

        List<String> listOne = readsDS
				.mapPartitions(new BwaPairedAlignmentDS(this.sparkSession.sparkContext(), bwa),Encoders.STRING() ).as(Encoders.STRING()).collectAsList();
        
		return listOne;
		
		//return readsRDD 
		//		.mapPartitionsWithIndex(new BwaPairedAlignment(readsRDD.context(), bwa), true)
		//		.collect();

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
		LOG.error("["+this.getClass().getName()+"] :: Starting BWA");
		Bwa bwa = new Bwa(this.options);

		List<String> returnedValues;
		if (bwa.isPairedReads()) {
			Dataset<Row> readsDS = handlePairedReadsSorting();
			//JavaRDD<Tuple2<String, String>> readsRDD = handlePairedReadsSorting();
			returnedValues = MapPairedBwa(bwa, readsDS);
			//returnedValues = MapPairedBwa(bwa, readsRDD);
			
			//System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			//returnedValues.forEach(System.out::println);
			//handlePairedReadsSorting
			//System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");


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
					LOG.error("JMAbuin:: SparkBWA :: Returned file ::" + returnedValues.get(i));
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
			LOG.error("["+this.getClass().getName()+"] :: SparkBWA:: Returned file ::" + outputFile);
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
        this.conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		//this.conf.set("textinputformat.record.delimiter","\n@");

		//The block size
		this.blocksize = this.conf.getLong("dfs.blocksize", 134217728);

		createOutputFolder();
		setTotalInputLength();

		//ContextCleaner cleaner = this.ctx.sc().cleaner().get();
	}
}