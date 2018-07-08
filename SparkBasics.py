import pyspark
from pyspark.sql import SparkSession
#import os

spark = SparkSession.builder.appName("Basics").getOrCreate()

path = 'C:/Users/Sonali/Desktop/MS Subjects/SPARK-UDEMY/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/'
#os.chdir(path)

df = spark.read.json(path+'people.json')
df.show()
df.printSchema()
df.columns
df.describe().show()

from pyspark.sql.types import (StructField, StringType, 
                               IntegerType, StructType)

data_schema = [StructField('age', IntegerType(), True),
               StructField('name', StringType(), True)]

final_struc = StructType(fields=data_schema)

df1= spark.read.json(path+'people.json', schema=final_struc)
df1.printSchema()
type(df1)
df1['age']
df1.select('age').show()

#check data
df1.head(3)
df.select(['age','name']).show()

#add coloumn, copy coloumn
df.withColumn('doubleage',df['age']*2).show()

df1.show()

#Rename column
df.withColumnRenamed('age', 'mynewAge').show()

#=====================SQL==will need a database====================
df1.createOrReplaceGlobalTempView('people')
results = spark.sql("SELECT * FROM people")
results.show()
#===============================================

df2 = spark.read.csv(path+'appl_stock.csv', inferSchema = True, header=True)
df2.show(2)
df2.printSchema()

#==============dataframe operation
df2.filter(df2['Close'] < 500).show()
df2.filter(df2['Close'] < 500).select('Volume').show()
df2.filter((df2['Close'] > 200) & (df2['Close'] > 500)).show()  #imp
#====date with low price=============
df2.filter(df2['Low'] == 197.16).show()
results = df2.filter(df2['Low'] == 197.16).collect() #check data in list
row = results[0]
row.asDict()['Volume']
#========================Groupby and Aggregate=================
df3 = spark.read.csv(path+'sales_info.csv', inferSchema=True, header=True)
df3.show(10) 
df3.printSchema()

df3.groupBy("Company").mean().show()
df3.groupBy("Company").max().show()
df3.groupBy("Company").min('Sales').show()
df3.agg({'Sales':'sum'}).show()

group_data = df3.groupBy('Company')
group_data.agg({'Sales':'max'}).show()

#===============================================================
from pyspark.sql.functions import countDistinct, avg, stddev, format_number

df3.select(countDistinct('Sales')).show() 
df3.select(avg('Sales').alias('AverageSales')).show() 

df3.select(stddev('Sales')).show()
#======================format and orderby==============================================
from pyspark.sql.functions import format_number
salesSTD = df3.select(stddev('Sales').alias('STD'))
salesSTD.show()

salesSTD.select(format_number('STD',2).alias('std')).show()

df3.orderBy(df3["Sales"].desc()).show()
#==========================dealng with missing data============================
spark = SparkSession.builder.appName("missingData").getOrCreate()

df4 = spark.read.csv(path+'ContainsNull.csv', inferSchema=True, header=True)
df4.show()
#Lets drop missing data
df4.na.drop(thresh=2).show() #need to 2 null to drop values
df4.na.drop(how='all').show() # need all null to drop

df4.na.drop(subset=['Sales']).show() # sales with null
#=========fill na values
df4.printSchema()
df4.na.fill('Fill Value').show()
df4.na.fill(0).show()
df4.na.fill('Fill Value', subset=['Name']).show()

#===fill with mean values====================
from pyspark.sql.functions import mean 
meanvalue = df4.select(mean(df4['Sales'])).collect()
meanSales= meanvalue[0][0]

df4.na.fill(meanSales, subset=['Sales']).show()
#===fill with means
df4.na.fill(df4.select(mean(df4['Sales'])).collect()[0][0], ['Sales']).show()

#==================Date and time
df5 = spark.read.csv(path+'appl_stock.csv', inferSchema = True, header=True)
df5.printSchema()
df5.show(2)

df5.select(['Date','Open']).show()
#===============DAte formatting
from pyspark.sql.functions import (dayofmonth, hour, 
                                   dayofyear, month, year,
                                   format_number, date_format)

df5.select(dayofmonth(df5['Date'])).show()
df5.select(hour(df5['Date'])).show()
df5.select(month(df5['Date'])).show()
#========average closing price per year
df5.select(year(df5['Date'])).show()
newdf = df5.withColumn('Year', year(df5['Date']))
result = newdf.groupBy('Year').mean().select(['Year', 'avg(Close)'])
new = result.withColumnRenamed('avg(Close)','Average Closing Price')  # rename coloumn
new.select(['Year',format_number('Average Closing Price',2)]).show() # format number

