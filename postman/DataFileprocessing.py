# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 16:07:23 2021

@author: amitk
"""

import os
import sys
import traceback
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import configparser



class DataFileprocessing:
    def __init__(self,data,dburl,table,user,password):
        self.data=data
        self.dburl=dburl
        self.table=table
        self.user=user
        self.password=password
        

    
    def write_into_db(self):
        
        #products table
        try:
            self.data.write.mode("overwrite")\
                .format("jdbc")\
                .option("url",self.dburl)\
                .option("dbtable",self.table)\
                .option("user",self.user)\
                .option("password",self.password)\
                .option("driver","oracle.jdbc.driver.OracleDriver")\
                .save()
        except:
            traceback.print_exc()
            print("failed to write the table in database")
            
            
    def write_into_db_aggre_table(self,aggregate_table):
        
        #aggregate table
        ag_table=self.data.groupBy("name").count().withColumnRenamed('count', 'no_of_products')
        ag_table.show()
        try:
            ag_table.write.mode("overwrite")\
                .format("jdbc")\
                .option("url",self.dburl)\
                .option("dbtable",aggregate_table)\
                .option("user",self.user)\
                .option("password",self.password)\
                .option("driver","oracle.jdbc.driver.OracleDriver")\
                .save()
        except:
            traceback.print_exc()
            print("failed to write the table in database")
            
        
            

    
    
                
    
    
        