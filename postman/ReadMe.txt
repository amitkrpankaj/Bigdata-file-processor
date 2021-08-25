# -*- coding: utf-8 -*-
"""
Created on Wed Aug 25 13:59:46 2021

@author: amitk
"""

A.Steps to run the code
______________________________________________________

1.Clone the file from github
2.see the Run cmd text file


B.Details of all tables
_______________________________________________________
1.first table is Product table that store all details such as name,sku,description:

Schema: See the screenshot no 248


2.Second table is aggregate table that store the details such as name,no_of_products :

Schema: See the screenshot no 250


C.Done from point to acheive 
____________________________________________________________
1.Followed as much as possible oops concept

2.Processed the large file and imported in single table

3.Created the aggregate table as mentiioned in task

4.Acheived the parallel processing and distribusted fashion to precess the large file

5. See the screenshot no 249 and 251 to see the sample 10 rows

D.Not Done from point to acheive
_____________________________________________________________
1.I did not understand the condition "Consider thinking about the scale of what should happen if the file is to be processed in 2 mins"
  onces I'll get clear with that condition then will implement
  
2."Support for updating existing products in the table based on `sku` as the primary key."
   for this there is two possible solution:
   
   2.1 ..First I can directly update table using sql query 
   2.2 ...Second way that I can write a update method in backend side and take input accordingly such that 
          list of columns,list of values,list of condition (SKU as primary key) 

E.What would you improve if given more days
___________________________________________________
Sure...I can make some changes and build a better system for large file processing
and also will finish the task that earlier not done