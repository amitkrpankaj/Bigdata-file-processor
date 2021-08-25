# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 16:44:12 2021

@author: amitk
"""
import os
import sys
import traceback
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from DataFileprocessing import *

if __name__ == "__main__":
    
    
    '''
    setMaster can be either local[],spark cluster such as yarn etc,or Kubernetes for
    task scheduling, memory management,task monitor'''
    
    
    
    #sparkcontext based sparkconf
    
    sc = SparkContext()
    
    #spark session 
    
    spark = SparkSession(sc)
    
    ''' as of now taking filenam,filepath directly but we can take it from end user'''
    
    filepath="C:/Users/amitk/Documents/postman"
    filename="products.csv"
    
    try:
        data=spark.read.csv(os.path.join(filepath,filename),header=True)
        data.show()
    except:
            traceback.print_exc()
            print("failed to load data from csv file")
    
    
    #create the object of class DataFileprocessing and pass data to save in db
    ''' as of now taking directly but we can take it from end user'''
    
    #give the database url where you want to save this table
    dburl="jdbc:oracle:thin:SYSTEM/oracle@localhost:1521/xe"
    #give the username of DB where you want to save this table
    user="SYSTEM"
    #give the password of DB where you want to save this table
    password="oracle"
    
    #give the table name as you want in database
    table="products_table"
    #object 1
    obj1 = DataFileprocessing(data,dburl,table,user,password)
    obj1.write_into_db()
    
    #give the table name as you want in database
    aggregate_table="aggregate_products_table"
    #object 1
    obj1.write_into_db_aggre_table(aggregate_table)
    
    spark.stop()
    
    