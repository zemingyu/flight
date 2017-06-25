#!/usr/bin/env python2.7
# import h2o
import zipfile
import os
import sys
from pyspark.sql import SparkSession
from IPython.display import display
from pyspark.sql.functions import regexp_extract, col, split, udf, \
                                 trim, when, from_unixtime, unix_timestamp, minute, hour, datediff, lit, array,\
                                 to_date
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType, ArrayType, StructType, StructField, LongType, TimestampType
import datetime
import argparse
import json
import glob, os, shutil
import pandas as pd
from pandas.io.json import json_normalize
from pyspark import SparkContext
from time import sleep
from math import floor
from os.path import join
from os import listdir, rmdir
from shutil import move
import csv
from time import sleep
from math import floor


pd.options.display.max_columns = 99

sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("Data ETL") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
		

def correct_stay_days(trip, stay_days):
    if trip == '1':
        return None
    else:
        return int(stay_days)

def correct_tickets_left(noOfTicketsLeft):
    if noOfTicketsLeft == 0:
        return 99
    else:
        return noOfTicketsLeft

		correct_stay_days_UDF = udf(correct_stay_days, IntegerType())    

def reformat_v1_0(flight, pqFolder, pqFileName):
	"""
		Read in the original v1.0 dataframe and save as a new parquet file compatible with v1.1
		@params:        
			flight		  - Required  : original v1.0 data(Spark DataFrame)        
			pqFolder      - Required  : folder to save the parquet files into (Str)        
			pqFileName    - Required  : parquet file name (Bool)                        
	"""
	flight2 = (flight.withColumn('stayDays', correct_stay_days_UDF(col('trip'), col('stay_days')))
					 .drop('stay_days')           
					 .withColumnRenamed('start_date', 'depDate')                 
					 .withColumn('depDate', to_date('depDate'))
					 .selectExpr('*', 'date_add(depDate, stayDays) as retDate')# this is when the return trip starts, might arrive a day later
					 .withColumnRenamed('from_city_name', 'fromCity')
					 .withColumnRenamed('to_city_name', 'toCity')                 
					 .withColumnRenamed('search_date', 'searchDate')                 
					 .withColumn('searchDate', to_date('searchDate'))
					 .withColumnRenamed('company', 'airlineName')                 
					 .withColumnRenamed('dep_time', 'departureTime')                                  
					 .withColumnRenamed('arr_time', 'arrivalTime')                                                   
					 .withColumn('duration_h', split(flight.duration,'h').getItem(0))
					 .withColumn('duration_m', F.substring_index(split(flight.duration,'h').getItem(1), 'm', 1))
	#                  .withColumn('duration', F.struct(col('duration_h'), col('duration_m')))
					 .withColumn('duration_m', (col('duration_h')*60 + col('duration_m')))
					 .drop('duration', 'duration_h', 'flight_number')
					 .withColumnRenamed('price_code', 'currencyCode')                                  
					 .withColumnRenamed('stop', 'stops')
					 .withColumn('stops', col('stops').cast('byte')) 
					 .withColumn('stop_info', split(col('stop_info'), ';'))
	#                  .withColumn('stop_duration', take_all_duration_UDF(col('stop_info')))
					 .withColumn('noOfTicketsLeft', correct_tickets_left_UDF('ticket_left'))
					 .withColumn('noOfTicketsLeft', col('noOfTicketsLeft').cast('byte')) 
					.drop('ticket_left')
				   .withColumnRenamed('table_name', 'tableName')
				   .withColumn('task_id', col('task_id').cast('long')) 
				   .withColumn('span_days', col('span_days').cast('integer')) 
					.select('price', 'version', 'searchDate', 'tableName', 'task_id', 'currencyCode', 
							'fromCity', 'toCity', 'trip', 'depDate', 'retDate',
							'stayDays', 
						   'departureTime', 'arrivalTime', 
							'airlineName',  'duration_m', 
							'flight_code', 'plane', 'stops', 'noOfTicketsLeft',
						   'airline_code', 'airline_codes',
						   'stop_info', 'span_days', 'power', 'video', 'wifi')                #'stop_duration', 
			  )

	flight2.repartition(1).write.parquet(os.path.join(pq_folder, pqFileName))

def txtToPq(inputFolder, pqFolder, pqFileName, searchString = "*.txt", append = True):
    """
    Read in all txt files in a folder, convert to parquet, and either append parquet or create new parquet
	This version is compatible with some of the v1.1 files inside s3://flight.price
    @params:
        inputFolder   - Required  : input folder that contains json line txt files (Str)        
        pqFolder      - Required  : folder to save the parquet files into (Str)        
        pqFileName    - Required  : parquet file name (Bool)        
        append        - Optional  : append to existing parquet or create new parquet 
        searchString  - Optional  : search string that identifies all the json line text files (Str)        
    """
    
    flightv1_1 = spark.read.json(os.path.join(inputFolder, searchString))
    
    flightv1_1_2 = (flightv1_1.withColumn('trip', col('trip').cast('string'))
                        .withColumn('stayDays', correct_stay_days_UDF(col('trip'), col('stayDays')))                    
                        .withColumn('depDate', to_date('depDate'))
                        .withColumn('searchDate', to_date('searchDate'))
                        .selectExpr('*', 'date_add(depDate, stayDays) as retDate')# this is when the return trip starts, might arrive a day later
                        .withColumn('airline_code', flightv1_1.flight_leg1.carrierSummary.airlineCodes.getItem(0))                   
                        .withColumn('airline_codes', flightv1_1.flight_leg1.carrierSummary.airlineCodes)                    
                        .withColumn('airline_codes_leg2', flightv1_1.flight_leg2.carrierSummary.airlineCodes)                    
                        .withColumn('departureTime', flightv1_1.flight_leg1.departureTime)
                        .withColumn('departureTime_leg2', flightv1_1.flight_leg2.departureTime)
                        .withColumn('arrivalTime', flightv1_1.flight_leg1.arrivalTime)
                        .withColumn('arrivalTime_leg2', flightv1_1.flight_leg2.arrivalTime)
    #                 .withColumn('check_bag_inc', flightv1_1.flight_leg1.arrivalTime)
                        .withColumn('airlineName', flightv1_1.flight_leg1.carrierSummary.airlineName)
                        .withColumn('airlineName_leg2', flightv1_1.flight_leg2.carrierSummary.airlineName)
                        .withColumn('duration_m', (F.unix_timestamp('arrivalTime', format=timeFmt) - 
                                                   F.unix_timestamp('departureTime', format=timeFmt))/60)                    
                    .withColumn('duration_m_leg2', (F.unix_timestamp('arrivalTime_leg2', format=timeFmt) - 
                                                   F.unix_timestamp('departureTime_leg2', format=timeFmt))/60)                    
    #                     .withColumn('duration', flightv1_1.timeline_leg1.getItem(1).duration)
                    .withColumn('airlineCode', flightv1_1.timeline_leg1.getItem(0).carrier.airlineCode)
                    .withColumn('flightNumber', flightv1_1.timeline_leg1.getItem(0).carrier.flightNumber.cast('string'))                
                    .select('*', F.concat(col('airlineCode'), col('flightNumber')).alias('flight_code'))
                    .drop('airlineCode', 'flightNumber')
                    .withColumn('plane', flightv1_1.timeline_leg1.getItem(0).carrier.plane)                
                    .withColumn('stops', flightv1_1.flight_leg1.stops.cast('byte'))                                
                    .withColumn('stops_leg2', flightv1_1.flight_leg2.stops.cast('byte'))                

    #                 .withColumn('stop_list', flightv1_1.flight_leg1.stop_list)# need to do more work                
                    .withColumn('stop_airport', take_all_level1_str(flightv1_1.flight_leg1.stop_list, lit('airport')))                                               
                    .withColumn('stop_duration', take_all_level1_str(flightv1_1.flight_leg1.stop_list, lit('duration')))                                               

    #                 .withColumn('stop_list_leg2', flightv1_1.flight_leg2.stop_list)               
                    .withColumn('stop_airport_leg2', take_all_level1_str(flightv1_1.flight_leg2.stop_list, lit('airport')))                                               
                    .withColumn('stop_duration_leg2', take_all_level1_str(flightv1_1.flight_leg2.stop_list, lit('duration')))                                               



                    .withColumn('noOfTicketsLeft', correct_tickets_left_UDF(flightv1_1.flight_leg1.carrierSummary.noOfTicketsLeft))
                    .withColumn('noOfTicketsLeft', col('noOfTicketsLeft').cast('byte'))                
                    .withColumn('noOfTicketsLeft_leg2', correct_tickets_left_UDF(flightv1_1.flight_leg2.carrierSummary.noOfTicketsLeft))
                    .withColumn('noOfTicketsLeft_leg2', col('noOfTicketsLeft_leg2').cast('byte'))
                    .withColumn('fromCityAirportCode', flightv1_1.flight_leg1.departureLocation.airportCode)                
                    .withColumn('toCityAirportCode', flightv1_1.flight_leg1.arrivalLocation.airportCode)
                    .withColumn('fromCityAirportCode_leg2', flightv1_1.flight_leg2.departureLocation.airportCode)
                    .withColumn('toCityAirportCode_leg2', flightv1_1.flight_leg2.arrivalLocation.airportCode)

                    # carrier leg 1
                    .withColumn('carrierAirProviderId', flightv1_1.flight_leg1.carrierSummary.airProviderId)
                    .withColumn('carrierAirlineImageFileName', flightv1_1.flight_leg1.carrierSummary.airlineImageFileName)
                    .withColumn('carrierMixedCabinClass', flightv1_1.flight_leg1.carrierSummary.mixedCabinClass)
                    .withColumn('carrierMultiStop', flightv1_1.flight_leg1.carrierSummary.multiStop)
                    .withColumn('carrierNextDayArrival', flightv1_1.flight_leg1.carrierSummary.nextDayArrival)

                    # carrier leg 2
                    .withColumn('carrierAirProviderId_leg2', flightv1_1.flight_leg2.carrierSummary.airProviderId)
                    .withColumn('carrierAirlineImageFileName_leg2', flightv1_1.flight_leg2.carrierSummary.airlineImageFileName)
                    .withColumn('carrierMixedCabinClass_leg2', flightv1_1.flight_leg2.carrierSummary.mixedCabinClass)
                    .withColumn('carrierMultiStop_leg2', flightv1_1.flight_leg2.carrierSummary.multiStop)
                    .withColumn('carrierNextDayArrival_leg2', flightv1_1.flight_leg2.carrierSummary.nextDayArrival)

                    ### Leg 1
                    ## Leg 1 departure
    #                 .withColumn('timeline_departureAirport', take_all_airport(flightv1_1.timeline_leg1, lit('departureAirport')))                               
                    .withColumn('timeline_departureAirport_cityState', take_all_level2_str(flightv1_1.timeline_leg1, lit('departureAirport'), lit('airportCityState')))
                    .withColumn('timeline_departureAirport_city', take_all_level2_str(flightv1_1.timeline_leg1, lit('departureAirport'), lit('city')))
                    .withColumn('timeline_departureAirport_code', take_all_level2_str(flightv1_1.timeline_leg1, lit('departureAirport'), lit('code')))
                    .withColumn('timeline_departureAirport_localName', take_all_level2_str(flightv1_1.timeline_leg1, lit('departureAirport'), lit('localName')))
                    .withColumn('timeline_departureAirport_longName', take_all_level2_str(flightv1_1.timeline_leg1, lit('departureAirport'), lit('longName')))
                    .withColumn('timeline_departureAirport_name', take_all_level2_str(flightv1_1.timeline_leg1, lit('departureAirport'), lit('name')))

                    .withColumn('timeline_departureTime', take_all_level2_str(flightv1_1.timeline_leg1, lit('departureTime'), lit('isoStr')))



                    ## Leg 1 arrival
                    .withColumn('timeline_arrivalAirport_cityState', take_all_level2_str(flightv1_1.timeline_leg1, lit('arrivalAirport'), lit('airportCityState')))
                    .withColumn('timeline_arrivalAirport_city', take_all_level2_str(flightv1_1.timeline_leg1, lit('arrivalAirport'), lit('city')))
                    .withColumn('timeline_arrivalAirport_code', take_all_level2_str(flightv1_1.timeline_leg1, lit('arrivalAirport'), lit('code')))
                    .withColumn('timeline_arrivalAirport_localName', take_all_level2_str(flightv1_1.timeline_leg1, lit('arrivalAirport'), lit('localName')))
                    .withColumn('timeline_arrivalAirport_longName', take_all_level2_str(flightv1_1.timeline_leg1, lit('arrivalAirport'), lit('longName')))
                    .withColumn('timeline_arrivalAirport_name', take_all_level2_str(flightv1_1.timeline_leg1, lit('arrivalAirport'), lit('name')))                

                    .withColumn('timeline_arrivalTime', take_all_level2_str(flightv1_1.timeline_leg1, lit('arrivalTime'), lit('isoStr')))

                    # distance
                    .withColumn('timeline_distance', take_all_level2_str(flightv1_1.timeline_leg1, lit('distance'), lit('formattedTotal')))

                    # carrier
                    .withColumn('timeline_plane', take_all_level2_str(flightv1_1.timeline_leg1, lit('carrier'), lit('plane')))

                    # brandedFareName
                    .withColumn('timeline_brandedFareName', take_all_level1_str(flightv1_1.timeline_leg1, lit('brandedFareName')))                               

                    # type
                    .withColumn('timeline_type', take_all_level1_str(flightv1_1.timeline_leg1, lit('type')))                               

                    ### Leg 2
                    ## Leg 2 departure
                    .withColumn('timeline_departureAirport_cityState_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('departureAirport'), lit('airportCityState')))
                    .withColumn('timeline_departureAirport_city_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('departureAirport'), lit('city')))
                    .withColumn('timeline_departureAirport_code_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('departureAirport'), lit('code')))
                    .withColumn('timeline_departureAirport_localName_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('departureAirport'), lit('localName')))
                    .withColumn('timeline_departureAirport_longName_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('departureAirport'), lit('longName')))
                    .withColumn('timeline_departureAirport_name_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('departureAirport'), lit('name')))

                    .withColumn('timeline_departureTime_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('departureTime'), lit('isoStr')))                


                    ## Leg 2 arrival
                    .withColumn('timeline_arrivalAirport_cityState_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('arrivalAirport'), lit('airportCityState')))
                    .withColumn('timeline_arrivalAirport_city_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('arrivalAirport'), lit('city')))
                    .withColumn('timeline_arrivalAirport_code_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('arrivalAirport'), lit('code')))
                    .withColumn('timeline_arrivalAirport_localName_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('arrivalAirport'), lit('localName')))
                    .withColumn('timeline_arrivalAirport_longName_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('arrivalAirport'), lit('longName')))
                    .withColumn('timeline_arrivalAirport_name_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('arrivalAirport'), lit('name')))                

                    .withColumn('timeline_arrivalTime_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('arrivalTime'), lit('isoStr')))

                    # distance
                    .withColumn('timeline_distance_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('distance'), lit('formattedTotal')))

                    # carrier
                    .withColumn('timeline_plane_leg2', take_all_level2_str(flightv1_1.timeline_leg2, lit('carrier'), lit('plane')))

                    # brandedFareName
                    .withColumn('timeline_brandedFareName_leg2', take_all_level1_str(flightv1_1.timeline_leg2, lit('brandedFareName')))                           

                    # type
                    .withColumn('timeline_type_leg2', take_all_level1_str(flightv1_1.timeline_leg2, lit('type')))                               
                    
                    
                    # create variables droppped from v1.0
                    .withColumn('span_days', lit(99))
                    .withColumn('power', lit(False))
                    .withColumn('video', lit(False))
                    .withColumn('wifi', lit(False))
                    .withColumn('stop_info', col('stop_airport')) #placeholder. can't figure out how to create struct literal



                    .select('price', 'version', 'searchDate', 'tableName', 'task_id', 'currencyCode', 
                            'fromCity', 'toCity', 'trip', 'depDate', 'retDate',
                            'stayDays', 
                           'departureTime', 'arrivalTime', 'departureTime_leg2', 'arrivalTime_leg2',
                            'airlineName', 'airlineName_leg2', 'duration_m', 'duration_m_leg2',                
                            'flight_code', 'plane', 'stops', 'stops_leg2', 'stop_airport', 'stop_duration', 'stop_airport_leg2', 'stop_duration_leg2',
                            'noOfTicketsLeft', 'noOfTicketsLeft_leg2',
                           'airline_code', 'airline_codes', 'airline_codes_leg2', 
                            'url', 'fromCityAirportCode', 'toCityAirportCode', 'fromCityAirportCode_leg2', 'toCityAirportCode_leg2',
                           'carrierAirProviderId', 'carrierAirlineImageFileName', 'carrierMixedCabinClass', 'carrierMultiStop', 'carrierNextDayArrival',
                            'carrierAirProviderId_leg2', 'carrierAirlineImageFileName_leg2', 'carrierMixedCabinClass_leg2', 'carrierMultiStop_leg2', 'carrierNextDayArrival_leg2',

                            ## leg 1
                            # departure
                            'timeline_departureAirport_cityState', 'timeline_departureAirport_city', 'timeline_departureAirport_code', 'timeline_departureAirport_localName', 
                            'timeline_departureAirport_longName', 'timeline_departureAirport_name',

                            'timeline_departureTime',

                            # arrival
                            'timeline_arrivalAirport_cityState', 'timeline_arrivalAirport_city', 'timeline_arrivalAirport_code', 'timeline_arrivalAirport_localName', 
                            'timeline_arrivalAirport_longName', 'timeline_arrivalAirport_name',

                            'timeline_arrivalTime',

                            'timeline_distance',
                            'timeline_plane',
                            'timeline_brandedFareName',
                            'timeline_type',

                            ## leg 2                        
                            # departure
                            'timeline_departureAirport_cityState_leg2', 'timeline_departureAirport_city_leg2', 'timeline_departureAirport_code_leg2', 'timeline_departureAirport_localName_leg2', 
                            'timeline_departureAirport_longName_leg2', 'timeline_departureAirport_name_leg2',

                            'timeline_departureTime_leg2',

                            # arrival
                            'timeline_arrivalAirport_cityState_leg2', 'timeline_arrivalAirport_city_leg2', 'timeline_arrivalAirport_code_leg2', 'timeline_arrivalAirport_localName_leg2', 
                            'timeline_arrivalAirport_longName_leg2', 'timeline_arrivalAirport_name_leg2',

                            'timeline_arrivalTime_leg2',

                            'timeline_distance_leg2',
                            'timeline_plane_leg2',
                            'timeline_brandedFareName_leg2',
                            'timeline_type_leg2',
                            
                            # variables dropped from v1.0
                            'span_days', 'power', 'video', 'wifi', 'stop_info'

                           )                
                   )

    if append:
        flightv1_1_2.repartition(1).write.mode('append').parquet(os.path.join(pqFolder, pqFileName))        
    else:
        flightv1_1_2.repartition(1).write.parquet(os.path.join(pqFolder, pqFileName))       


def txtToPq_v2(inputFolder, pqFolder, pqFileName, searchString = "*.txt", append = True):
    """
    Read in all txt files in a folder, convert to parquet, and either append parquet or create new parquet
	This version is compatible with some of the v1.1 files inside s3://flight.price.11
	Main difference: leg1 is renamed to leg1	
    @params:
        inputFolder   - Required  : input folder that contains json line txt files (Str)        
        pqFolder      - Required  : folder to save the parquet files into (Str)        
        pqFileName    - Required  : parquet file name (Bool)        
        append        - Optional  : append to existing parquet or create new parquet 
        searchString  - Optional  : search string that identifies all the json line text files (Str)        
    """
    
    flightv1_1 = spark.read.json(os.path.join(inputFolder, searchString))
    
    flightv1_1_2 = (flightv1_1.withColumn('trip', col('trip').cast('string'))
                            .withColumn('stayDays', correct_stay_days_UDF(col('trip'), col('stayDays')))                    
                            .withColumn('depDate', to_date('depDate'))
                            .withColumn('searchDate', to_date('searchDate'))
                            .selectExpr('*', 'date_add(depDate, stayDays) as retDate')# this is when the return trip starts, might arrive a day later
                            .withColumn('airline_code', flightv1_1.leg1.carrierSummary.airlineCodes.getItem(0))                   
                            .withColumn('airline_codes', flightv1_1.leg1.carrierSummary.airlineCodes)                    
                            .withColumn('airline_codes_leg2', flightv1_1.leg2.carrierSummary.airlineCodes)                    
                            .withColumn('departureTime', flightv1_1.leg1.departureTime.isoStr)
                            .withColumn('departureTime_leg2', flightv1_1.leg2.departureTime.isoStr)
                            .withColumn('arrivalTime', flightv1_1.leg1.arrivalTime.isoStr)
                            .withColumn('arrivalTime_leg2', flightv1_1.leg2.arrivalTime.isoStr)
        #                 .withColumn('check_bag_inc', flightv1_1.leg1.arrivalTime)
                            .withColumn('airlineName', flightv1_1.leg1.carrierSummary.airlineName)
                            .withColumn('airlineName_leg2', flightv1_1.leg2.carrierSummary.airlineName)
                            .withColumn('duration_m', (F.unix_timestamp('arrivalTime', format=timeFmt) - 
                                                       F.unix_timestamp('departureTime', format=timeFmt))/60)                    
                        .withColumn('duration_m_leg2', (F.unix_timestamp('arrivalTime_leg2', format=timeFmt) - 
                                                       F.unix_timestamp('departureTime_leg2', format=timeFmt))/60)                    
        #                     .withColumn('duration', flightv1_1.timeline1.getItem(1).duration)
                        .withColumn('airlineCode', flightv1_1.timeline1.getItem(0).carrier.airlineCode)
                        .withColumn('flightNumber', flightv1_1.timeline1.getItem(0).carrier.flightNumber.cast('string'))                
                        .select('*', F.concat(col('airlineCode'), col('flightNumber')).alias('flight_code'))
                        .drop('airlineCode', 'flightNumber')
                        .withColumn('plane', flightv1_1.timeline1.getItem(0).carrier.plane)                
                        .withColumn('stops', flightv1_1.leg1.stops.cast('byte'))                                
                        .withColumn('stops_leg2', flightv1_1.leg2.stops.cast('byte'))                

        #                 .withColumn('stop_list', flightv1_1.leg1.stop_list)# need to do more work                
                        .withColumn('stop_airport', take_all_level1_str(flightv1_1.leg1.stop_list, lit('airport')))                                               
                        .withColumn('stop_duration', take_all_level1_str(flightv1_1.leg1.stop_list, lit('duration')))                                               

        #                 .withColumn('stop_list_leg2', flightv1_1.leg2.stop_list)               
                        .withColumn('stop_airport_leg2', take_all_level1_str(flightv1_1.leg2.stop_list, lit('airport')))                                               
                        .withColumn('stop_duration_leg2', take_all_level1_str(flightv1_1.leg2.stop_list, lit('duration')))                                               


                        .withColumn('noOfTicketsLeft', correct_tickets_left_UDF(flightv1_1.leg1.carrierSummary.noOfTicketsLeft))
                        .withColumn('noOfTicketsLeft', col('noOfTicketsLeft').cast('byte'))                
                        .withColumn('noOfTicketsLeft_leg2', correct_tickets_left_UDF(flightv1_1.leg2.carrierSummary.noOfTicketsLeft))
                        .withColumn('noOfTicketsLeft_leg2', col('noOfTicketsLeft_leg2').cast('byte'))
                        .withColumn('fromCityAirportCode', flightv1_1.leg1.departureLocation.airportCode)                
                        .withColumn('toCityAirportCode', flightv1_1.leg1.arrivalLocation.airportCode)
                        .withColumn('fromCityAirportCode_leg2', flightv1_1.leg2.departureLocation.airportCode)
                        .withColumn('toCityAirportCode_leg2', flightv1_1.leg2.arrivalLocation.airportCode)

                        # carrier leg 1
                        .withColumn('carrierAirProviderId', flightv1_1.leg1.carrierSummary.airProviderId)
                        .withColumn('carrierAirlineImageFileName', flightv1_1.leg1.carrierSummary.airlineImageFileName)
                        .withColumn('carrierMixedCabinClass', flightv1_1.leg1.carrierSummary.mixedCabinClass)
                        .withColumn('carrierMultiStop', flightv1_1.leg1.carrierSummary.multiStop)
                        .withColumn('carrierNextDayArrival', flightv1_1.leg1.carrierSummary.nextDayArrival)

                        # carrier leg 2
                        .withColumn('carrierAirProviderId_leg2', flightv1_1.leg2.carrierSummary.airProviderId)
                        .withColumn('carrierAirlineImageFileName_leg2', flightv1_1.leg2.carrierSummary.airlineImageFileName)
                        .withColumn('carrierMixedCabinClass_leg2', flightv1_1.leg2.carrierSummary.mixedCabinClass)
                        .withColumn('carrierMultiStop_leg2', flightv1_1.leg2.carrierSummary.multiStop)
                        .withColumn('carrierNextDayArrival_leg2', flightv1_1.leg2.carrierSummary.nextDayArrival)

                        ### Leg 1
                        ## Leg 1 departure
        #                 .withColumn('timeline_departureAirport', take_all_airport(flightv1_1.timeline1, lit('departureAirport')))                               
                        .withColumn('timeline_departureAirport_cityState', take_all_level2_str(flightv1_1.timeline1, lit('departureAirport'), lit('airportCityState')))
                        .withColumn('timeline_departureAirport_city', take_all_level2_str(flightv1_1.timeline1, lit('departureAirport'), lit('city')))
                        .withColumn('timeline_departureAirport_code', take_all_level2_str(flightv1_1.timeline1, lit('departureAirport'), lit('code')))
                        .withColumn('timeline_departureAirport_localName', take_all_level2_str(flightv1_1.timeline1, lit('departureAirport'), lit('localName')))
                        .withColumn('timeline_departureAirport_longName', take_all_level2_str(flightv1_1.timeline1, lit('departureAirport'), lit('longName')))
                        .withColumn('timeline_departureAirport_name', take_all_level2_str(flightv1_1.timeline1, lit('departureAirport'), lit('name')))

                        .withColumn('timeline_departureTime', take_all_level2_str(flightv1_1.timeline1, lit('departureTime'), lit('isoStr')))



                        ## Leg 1 arrival
                        .withColumn('timeline_arrivalAirport_cityState', take_all_level2_str(flightv1_1.timeline1, lit('arrivalAirport'), lit('airportCityState')))
                        .withColumn('timeline_arrivalAirport_city', take_all_level2_str(flightv1_1.timeline1, lit('arrivalAirport'), lit('city')))
                        .withColumn('timeline_arrivalAirport_code', take_all_level2_str(flightv1_1.timeline1, lit('arrivalAirport'), lit('code')))
                        .withColumn('timeline_arrivalAirport_localName', take_all_level2_str(flightv1_1.timeline1, lit('arrivalAirport'), lit('localName')))
                        .withColumn('timeline_arrivalAirport_longName', take_all_level2_str(flightv1_1.timeline1, lit('arrivalAirport'), lit('longName')))
                        .withColumn('timeline_arrivalAirport_name', take_all_level2_str(flightv1_1.timeline1, lit('arrivalAirport'), lit('name')))                

                        .withColumn('timeline_arrivalTime', take_all_level2_str(flightv1_1.timeline1, lit('arrivalTime'), lit('isoStr')))

                        # distance
                        .withColumn('timeline_distance', take_all_level2_str(flightv1_1.timeline1, lit('distance'), lit('formattedTotal')))

                        # carrier
                        .withColumn('timeline_plane', take_all_level2_str(flightv1_1.timeline1, lit('carrier'), lit('plane')))

                        # brandedFareName
                        .withColumn('timeline_brandedFareName', take_all_level1_str(flightv1_1.timeline1, lit('brandedFareName')))                               

                        # type
                        .withColumn('timeline_type', take_all_level1_str(flightv1_1.timeline1, lit('type')))                               

                        ### Leg 2
                        ## Leg 2 departure
                        .withColumn('timeline_departureAirport_cityState_leg2', take_all_level2_str(flightv1_1.timeline2, lit('departureAirport'), lit('airportCityState')))
                        .withColumn('timeline_departureAirport_city_leg2', take_all_level2_str(flightv1_1.timeline2, lit('departureAirport'), lit('city')))
                        .withColumn('timeline_departureAirport_code_leg2', take_all_level2_str(flightv1_1.timeline2, lit('departureAirport'), lit('code')))
                        .withColumn('timeline_departureAirport_localName_leg2', take_all_level2_str(flightv1_1.timeline2, lit('departureAirport'), lit('localName')))
                        .withColumn('timeline_departureAirport_longName_leg2', take_all_level2_str(flightv1_1.timeline2, lit('departureAirport'), lit('longName')))
                        .withColumn('timeline_departureAirport_name_leg2', take_all_level2_str(flightv1_1.timeline2, lit('departureAirport'), lit('name')))

                        .withColumn('timeline_departureTime_leg2', take_all_level2_str(flightv1_1.timeline2, lit('departureTime'), lit('isoStr')))                


                        ## Leg 2 arrival
                        .withColumn('timeline_arrivalAirport_cityState_leg2', take_all_level2_str(flightv1_1.timeline2, lit('arrivalAirport'), lit('airportCityState')))
                        .withColumn('timeline_arrivalAirport_city_leg2', take_all_level2_str(flightv1_1.timeline2, lit('arrivalAirport'), lit('city')))
                        .withColumn('timeline_arrivalAirport_code_leg2', take_all_level2_str(flightv1_1.timeline2, lit('arrivalAirport'), lit('code')))
                        .withColumn('timeline_arrivalAirport_localName_leg2', take_all_level2_str(flightv1_1.timeline2, lit('arrivalAirport'), lit('localName')))
                        .withColumn('timeline_arrivalAirport_longName_leg2', take_all_level2_str(flightv1_1.timeline2, lit('arrivalAirport'), lit('longName')))
                        .withColumn('timeline_arrivalAirport_name_leg2', take_all_level2_str(flightv1_1.timeline2, lit('arrivalAirport'), lit('name')))                

                        .withColumn('timeline_arrivalTime_leg2', take_all_level2_str(flightv1_1.timeline2, lit('arrivalTime'), lit('isoStr')))

                        # distance
                        .withColumn('timeline_distance_leg2', take_all_level2_str(flightv1_1.timeline2, lit('distance'), lit('formattedTotal')))

                        # carrier
                        .withColumn('timeline_plane_leg2', take_all_level2_str(flightv1_1.timeline2, lit('carrier'), lit('plane')))

                        # brandedFareName
                        .withColumn('timeline_brandedFareName_leg2', take_all_level1_str(flightv1_1.timeline2, lit('brandedFareName')))                           

                        # type
                        .withColumn('timeline_type_leg2', take_all_level1_str(flightv1_1.timeline2, lit('type')))                               

                        # create variables droppped from v1.0
                        .withColumn('span_days', lit(99))
                        .withColumn('power', lit(False))
                        .withColumn('video', lit(False))
                        .withColumn('wifi', lit(False))
                        .withColumn('stop_info', col('stop_airport')) #placeholder. can't figure out how to create struct literal


                        .select('price', 'version', 'searchDate', 'tableName', 'task_id', 'currencyCode', 
                                'fromCity', 'toCity', 'trip', 'depDate', 'retDate',
                                'stayDays', 
                               'departureTime', 'arrivalTime', 'departureTime_leg2', 'arrivalTime_leg2',
                                'airlineName', 'airlineName_leg2', 'duration_m', 'duration_m_leg2',                
                                'flight_code', 'plane', 'stops', 'stops_leg2', 'stop_airport', 'stop_duration', 'stop_airport_leg2', 'stop_duration_leg2',
                                'noOfTicketsLeft', 'noOfTicketsLeft_leg2',
                               'airline_code', 'airline_codes', 'airline_codes_leg2', 
                                'fromCityAirportCode', 'toCityAirportCode', 'fromCityAirportCode_leg2', 'toCityAirportCode_leg2',
                               'carrierAirProviderId', 'carrierAirlineImageFileName', 'carrierMixedCabinClass', 'carrierMultiStop', 'carrierNextDayArrival',
                                'carrierAirProviderId_leg2', 'carrierAirlineImageFileName_leg2', 'carrierMixedCabinClass_leg2', 'carrierMultiStop_leg2', 'carrierNextDayArrival_leg2',
                                #'url',

                                ## leg 1
                                # departure
                                'timeline_departureAirport_cityState', 'timeline_departureAirport_city', 'timeline_departureAirport_code', 'timeline_departureAirport_localName', 
                                'timeline_departureAirport_longName', 'timeline_departureAirport_name',

                                'timeline_departureTime',

                                # arrival
                                'timeline_arrivalAirport_cityState', 'timeline_arrivalAirport_city', 'timeline_arrivalAirport_code', 'timeline_arrivalAirport_localName', 
                                'timeline_arrivalAirport_longName', 'timeline_arrivalAirport_name',

                                'timeline_arrivalTime',

                                'timeline_distance',
                                'timeline_plane',
                                'timeline_brandedFareName',
                                'timeline_type',

                                ## leg 2                        
                                # departure
                                'timeline_departureAirport_cityState_leg2', 'timeline_departureAirport_city_leg2', 'timeline_departureAirport_code_leg2', 'timeline_departureAirport_localName_leg2', 
                                'timeline_departureAirport_longName_leg2', 'timeline_departureAirport_name_leg2',

                                'timeline_departureTime_leg2',

                                # arrival
                                'timeline_arrivalAirport_cityState_leg2', 'timeline_arrivalAirport_city_leg2', 'timeline_arrivalAirport_code_leg2', 'timeline_arrivalAirport_localName_leg2', 
                                'timeline_arrivalAirport_longName_leg2', 'timeline_arrivalAirport_name_leg2',

                                'timeline_arrivalTime_leg2',

                                'timeline_distance_leg2',
                                'timeline_plane_leg2',
                                'timeline_brandedFareName_leg2',
                                'timeline_type_leg2',

                                # variables dropped from v1.0
                                'span_days', 'power', 'video', 'wifi', 'stop_info'
                               )                
                       )


    if append:
        flightv1_1_2.repartition(1).write.mode('append').parquet(os.path.join(pqFolder, pqFileName))        
    else:
        flightv1_1_2.repartition(1).write.parquet(os.path.join(pqFolder, pqFileName))   

def unzip_files(dir_in, dir_out, extension):
    os.chdir(dir_in) # change directory from working dir to dir with files
    for subdir, dirs, files in os.walk(dir_in):
        for item in files:
            if item.endswith(extension): # check for ".zip" extension
                file_name = os.path.join(subdir, item)
                zip_ref = zipfile.ZipFile(file_name) # create zipfile object
                zip_ref.extractall(dir_out) # extract file to dir
                zip_ref.close() # close file             

                
def clear_folder(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)

        # recreate the folder after deletion
        if not os.path.exists(folder):
            os.makedirs(folder)

            
# Print iterations progress
# https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
#     print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix))        
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix))
    # Print New Line on Complete
    if iteration == total: 
        print()
		
def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size/(1024*1024*1024)


def get_zip_list()
	s3_client = boto3.client("s3")
	s3 = boto3.resource('s3')
	bucket = s3.Bucket('flight.price.11')

	## make a list
	i = 0
	l = 12661 # total number of zip files obtained using the following command
	#! aws s3 ls s3://flight.price.11/ --recursive | wc -l    
	s3_files = list()
	# Initial call to print 0% progress
	printProgressBar(i, l, prefix = 'Progress:', suffix = 'Complete', decimals = 1, length = 50)

	for item in bucket.objects.all():    
		# define s3 file name
		s3_file = item.key    
		s3_files.append(s3_file)
		
		sleep(0.1)
		# Update Progress Bar
		i += 1
		if i % floor(l / 200) == 0:
			printProgressBar(i, l, prefix = 'Progress:', suffix = 'Complete', decimals = 1, length = 50)    
	
	return s3_files
	
def main():
	# create UDF's that will be used by spark sql
	timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSS"
	correct_tickets_left_UDF = udf(correct_tickets_left, IntegerType())	
	take_all_level2_str = udf(lambda rows, a, b: None if rows is None else [None if row is None else row[a][b] for row in rows], ArrayType(StringType()))
	take_all_level1_str = udf(lambda rows, a: None if rows is None else [None if row is None else row[a] for row in rows], ArrayType(StringType()))
	take_all_duration_UDF = udf(lambda rows: None if rows is None else [None if row is None else row.split(":", 1)[1].replace("h", "h:") for row in rows], ArrayType(StringType()))


	# adjust these folder locations
	zip_folder = '/home/ubuntu/s3/zip/'
	txt_folder = '/home/ubuntu/s3/txt/'
	txt_exception_folder = '/home/ubuntu/s3/comb/txt_exception/'
	pq_folder = '/home/ubuntu/s3/pq_v1_1/'
	txt_new_exception_folder = '/home/ubuntu/s3/txt_exception/'	
	
	# get list of zip files to be transformed
	zip_list = get_zip_list()
	
	# initial setting for the main loop
	start = 67 # after failure, continue from this element of the list
	chunk_size = 1 # recommend setting this to about 34. 34 zip files will be consolidated to a 100mb parquet partition.
	remaining_list = zip_list[0][start:] #already processed 22
	# print([remaining_list[i:i+chunk_size] for i in range(0, len(remaining_list), chunk_size)])	
	
	
	### Main loop
	i = 0
	l = len(zip_list)
	
	# Initial call to print 0% progress
	printProgressBar(i, l, prefix = 'Progress:', suffix = 'Complete', decimals = 1, length = 50)

	for j in range(0, len(remaining_list), chunk_size):
		print("started " + str(start+j) + " out of " + str(l))		
		for item in remaining_list[j:j+chunk_size]:           
	#     for item in remaining_list[j:j+1]:   

			# clear cache
			spark.catalog.clearCache()
			
			# clear working folder
			clear_folder(zip_folder)
			clear_folder(txt_folder)   
			
			# define s3 file name
			s3_file = item    
			s3_file_new_name = s3_file.replace('/', '__')

			 # download zip
			s3_client.download_file('flight.price.11', s3_file, zip_folder + s3_file_new_name)

			# extract to txt
			unzip_files(zip_folder, txt_folder, '.zip')       

			# if necessary move subfolder contents to parent folder        
			try:
				for filename in listdir(join(txt_folder, 'final_results')):
					move(join(txt_folder, 'final_results', filename), join(txt_folder, filename))
				rmdir(join(txt_folder, 'final_results'))
			except Exception as e:
				print(e)
			
			print("finished extracting " + s3_file_new_name)
			print(get_size(txt_folder))

				
			# convert to parquet and append to existing parquet
			try:
				print("started transforming " + s3_file_new_name)
				txtToPq_v2(inputFolder = txt_folder, pqFolder = pq_folder,
								pqFileName = s3_file_new_name.replace(".zip", ".pq"), searchString = "*.txt", append = False)        
			except:
				for filename in listdir(txt_folder):
					move(join(txt_folder, filename), join(txt_new_exception_folder, filename))

			print("finished transforming " + s3_file_new_name)

			sleep(0.1)
			# Update Progress Bar
			i += 1
			if i % floor(l / 1000) == 0:
				printProgressBar(i, l, prefix = 'Progress:', suffix = 'Complete', decimals = 1, length = 50)    

		# ZY 20170625 to do: we can write some code to consolidate the individual pq files here		

