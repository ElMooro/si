Economic Measures Time Series Dataset
                     
Description
===========
The file provided along with this README is a comma separated value
file containing the most recently released data for economic timeseries.

All other available data sets can be found at:    
	http://wwww.census.gov/econ/currendata/datasets

The data can be dynamically queried at:    
	http://wwww.census.gov/econ/currendata/

The Time Series/Trend Charts tool makes it very easy to find and use
data on the economic indicators and related data sets.  This tool gives
public and private users an easy way to access time series data
associated with economic measures; thereby, facilitating their
assessment of economic performance and informing business investment and
policy decisions based on trend analyses of economic data.
                     
File Structure
==============

The data file is separated into eight sections using two consecutive
line breaks.  
                     
                     
CATEGORIES Section  
------------------  

The categories section contains the top level categories or industries  
into which the data is classified.  

The order in which the categories are listed is important because it, 
along with the values in the cat_indent column, defines the hierarchy  
of data point classifications. For example:
    
	Hierarchy                     Category Rows    
	---------                     -------------    
	Animals                       1,ANIM,Animals,0    
	|                             2,CAN,Canines,1    
	+-> Canines                   3,DOG,Dogs,2    
	|  |                          4,WOLF,Wolves,2    
	|  +-> Dogs                   5,FEL,Felines,1    
	|  |                          6,CAT,"House Cats",2    
	|  +-> Wolves                 7,LION,Lions,2    
	|    
	+-> Felines       
		|       
		+-> House Cats       
		|       
		+-> Lions  

Columns:    

	cat_idx		Index used to associate a data value with the category.    

	cat_code    Abbreviated category code.    

	cat_desc    Category label.    

	cat_indent  The hierarchy/indent level of the label when listing the                
                categories.  

DATA TYPES Section  
------------------  

The data type section provides the specific item and type of estimate  
in the time series, along with units of measure.  

Columns:    
	
	dt_idx      Index used to associate a data value with the data type.    
	
	dt_code     Abbreviated data type code.    
	
	dt_desc     The name of the item and type of estimate in the time                
				series (e.g., sales- monthly and sales- monthly percent                
				change)    
	
	dt_unit     The unit of measure code for this data type. Below is 
				a list of possible codes and their meanings:                
                MLN$   Millions of Dollars                
                BLN$   Billions of Dollars                
                K$     Thousands of Dollars                
                PCT    Percent                
                UNITS  Units                
                RATIO  Ratio                
                MO     Number of Months                
				DOL    Dollars                
                K      Thousands of Units                
                CP$    Cents per dollar                
                %PTS   Percentage Points                
                CENTS  Cents  
                     
ERROR TYPES Section  
-------------------  

The error types section provides estimates of sampling variability for  
the items/estimates in the time series. These values will only be  
available if the program publishes these measures; they are not  
available for census programs such as Foreign Trade or non-probability  
samples such as Manufacturers' Shipments, Inventories, and Orders  (M3). 
Datasets which do not publish this type of data will not contain this section.  

Standard errors (SEs) are provided for monthly or quarterly estimates  
of totals. Coefficients of variation (CV)/Relative Standard Errors  (RSE) 
are provided for percent change estimates. The CV /RSE is  
computed as the SE/EST (EST = estimated percent change)  

Columns:
    
	et_idx      Index used to associate a data value with the error type.    

	et_code     Abbreviated error type code.    
	
	et_desc     The name of the item and type of estimate of sampling                
				variability in the time series (e.g., coefficients of                
				variation of sales and standard errors of sales)    

	et_unit     The unit of measure code for this error type. Below is 
				a list of possible codes and their meanings:                

				MLN$   Millions of Dollars                
                BLN$   Billions of Dollars                
                K$     Thousands of Dollars                
                PCT    Percent                
                UNITS  Units                
                RATIO  Ratio                
                MO     Number of Months                
                DOL    Dollars                
                K      Thousands of Units                
                CP$    Cents per dollar                
                %PTS   Percentage Points                
                CENTS  Cents  
                     
GEO LEVELS Section  
------------------  

The geo levels section contains the geographic region associated with  
data values.  Geographic breakdowns are not available for every time  
series.  

Columns:    

	geo_idx     Index used to associate a data value with the
    			geographical level.    

	geo_code    Abbreviated geo level code.    


	geo_desc    Geographic level label.  
					 

TIME PERIODS Section  
--------------------  

The time periods section contains the dates for each estimate in the  
time series.  

Columns:    

	per_idx     Index used to associate a data value with the time                
				period.    

	per_name    The name of the time period. Values for this field can                
				consist of the following formats depending on the                
				release frequency for the time series:                
				
				Frequency       per_name Format                
				---------       ---------------                
				monthly         MMMYYYY (e.g. Jan2019, Dec2018)                
				quarterly       QXYYYY (e.g. Q12019, Q42018)                
				annually        YYYY (e.g. 1992, 2019)  
					 
NOTES Section  
-------------  

The notes section contains HTML formatted text that time series  
providers provide along with their data sets. This text often contains  
links back to their program areas and explanatory text on their  
methodology or special data values.  

For information on the reliability and use of the data, including  
important notes on estimation and sampling variance, seasonal  
adjustment, measures of sampling variability, and other information  
pertinent to this economic indicator, visit the individual program's  
webpages.  
					 
DATA UPDATED ON Section  
-----------------------  

This section is important because many programs publish revised  
estimates as more data becomes available, and the revised estimates  
can be very different from their originally published (preliminary)  
values.  
					 
DATA Section  
------------  

The data section contains the data values sorted by time period. The  
values are linked to their corresponding metadata using the index  
columns.  

Columns:    

	per_idx     Index used to associate a data value with the time                
				period.    

	cat_idx     Index used to associate a data value with the category.    

	dt_idx      Index used to associate a data value with the data type.    

	et_idx      Index used to associate a data value with the error                
				type. This column is only present if the time series                
				contains error data. Any non-zero value in this column                
				designates the corresponding data value as error data.                
				When the value in this column is non-zero, the dt_idx                
				column will always be zero.    

	geo_idx     Index used to associate a data value with the                
				geographical level.    
	is_adj      Identifies whether the value is seasonally adjusted or                
				not. A value of 1 indicates that it is adjusted, 0 means                
				it is not.    

	val         The data value.
					 

Data Usage and Attribution
==========================
The time series provided represent official statistics released by the
U.S. Census Bureau. Do not alter the precision of data values when
storing or formatting them for display, or the results may disagree with
the official releases.

All products developed using these data files must attribute the datasource 
as the U.S. Census Bureau and must contain the originating
program name.
					 
Contacts
========

- adeline.tran@census.gov