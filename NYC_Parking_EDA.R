# Loading Libraries
library(SparkR)
library(ggplot2)

# Initialising Spark
sparkR.session(master = "local")


# Reading Data
parking_masterframe_2015 <- read.df("s3://parking-tickets-casestudy-42/parking-2015/Parking_Violations_Issued_-_Fiscal_Year_2015.csv",
                                    source = "csv", inferSchema = "true", header = "true")

parking_masterframe_2016 <- read.df("s3://parking-tickets-casestudy-42/parking-2016/Parking_Violations_Issued_-_Fiscal_Year_2016.csv",
                                    source = "csv", inferSchema = "true", header = "true")

parking_masterframe_2017 <- read.df("s3://parking-tickets-casestudy-42/parking-2017/Parking_Violations_Issued_-_Fiscal_Year_2017.csv",
                                    source = "csv", inferSchema = "true", header = "true")


### Verifying if the data is properly loaded.
head(parking_masterframe_2015)
head(parking_masterframe_2016)
head(parking_masterframe_2017)

## Creating a lazily evaluated view for Spark SQL

createOrReplaceTempView(parking_masterframe_2015, "parking_masterframe_2015_sql")

createOrReplaceTempView(parking_masterframe_2016, "parking_masterframe_2016_sql")

createOrReplaceTempView(parking_masterframe_2017, "parking_masterframe_2017_sql")


# nrows_2015 = 11809233
nrows_2015 <- nrow(parking_masterframe_2015)
nrows_2015

# nrows_2016 = 10626899
nrows_2016 <- nrow(parking_masterframe_2016)
nrows_2016

# nrows_2017 = 10803028
nrows_2017 <- nrow(parking_masterframe_2017)
nrows_2017

# ncols_2015 = 51L 
ncols_2015 <- ncol(parking_masterframe_2015)
ncols_2015

# ncols_2016 = 51L
ncols_2016 <- ncol(parking_masterframe_2016)
ncols_2016

# ncols_2017 = 43L
ncols_2017 <- ncol(parking_masterframe_2017)
ncols_2017

####################################### Data Cleaning ###############################################

### Dropping duplicated values of summon numbers for each dataset, since it is a primary
### key column, and has to be unique.

### Dropping Duplicated Summons Number for year 2015:

cleaned_master_frame_2015 <- dropDuplicates(parking_masterframe_2015, c("Summons Number"))

total_entries_2015 <- nrow(cleaned_master_frame_2015)

total_entries_2015   # 10951256

## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(cleaned_master_frame_2015, "cleaned_master_frame_2015_sql")

### Dropping Duplicated Summons Number for year 2016:

cleaned_master_frame_2016 <- dropDuplicates(parking_masterframe_2016, c("Summons Number"))

total_entries_2016 <- nrow(cleaned_master_frame_2016)

total_entries_2016   # 10626899

## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(cleaned_master_frame_2016, "cleaned_master_frame_2016_sql")

### Dropping Duplicated Summons Number for year 2017:

cleaned_master_frame_2017 <- dropDuplicates(parking_masterframe_2017, c("Summons Number"))

total_entries_2017 <- nrow(cleaned_master_frame_2017)   

total_entries_2017  # 10803028

## Creating a lazily evaluated view for Spark SQL

createOrReplaceTempView(cleaned_master_frame_2017, "cleaned_master_frame_2017_sql")

## As per the official NYC Gov Website here: https://www1.nyc.gov/site/omb/faq/frequently-asked-questions.page
## The City's fiscal year begins on July 1st of one calendar year and 
## ends on June 30th of the following calendar year.


####### Checking the Quality of Data by having a look at the Issue Date, Since the data set is 
####### provided based on Issued Year. Hence for each Fiscal Year, the Issued data should lie 
####### within the respective Fiscal Year's Dates. For e.g, the Fiscal Year for 2015 began on 
####### July-1-2014 and ended on June-30-2015, and same way for years 2016 and 2017.

### Checking invalid values for years 2015

grouped_years_2015 <- SparkR::sql("select YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as Year, 
                                  count(*) as no_of_entries
                                  from cleaned_master_frame_2015_sql
                                  group by YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)))
                                  order by count(*) desc")

grouped_years_df_2015 <- collect(grouped_years_2015)

grouped_years_df_2015   

# As we can see, there are many years other than year 2014 and 2015. However,
# We are only interested in the entries between July-1-2014 to June-30-2015. Rest all donot count
# into Fiscal Year 2015, as per above mentioned NYC FAQ link . Hence that needs to be cleaned.

### Counting the entries which donot lie in the fiscal range defined above

invalid_entries_2015 <- SparkR::sql("select count(*) as invalid_entries_2015
                                    from cleaned_master_frame_2015_sql
                                    WHERE TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)) 
                                    NOT BETWEEN '2014-07-01' AND '2015-06-30'")

num_invalid_entries_2015 <- collect(invalid_entries_2015)
num_invalid_entries_2015  #= 353221 

### We need to remove these invalid entries. Also, the total_entries_2015 = 10951256 and
### num_invalid_entries_2015 = 353321, which is 353321/10951256 = 3.22% of the total dataset for 2015.
### Hence, it is fairly reasonable amount to remove, and wont affect the analysis of dataset much.

# Keeping only the entries whose Issue data lie in the range of Fiscal Year 2015.
parking_temp_2015 <- SparkR::sql("select *
                                 from cleaned_master_frame_2015_sql
                                 WHERE TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)) 
                                 BETWEEN '2014-07-01' AND '2015-06-30'")


## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(parking_temp_2015, "parking_temp_2015_sql")


# Dropping Null values in Summons Number column if any
parking_2015 <- SparkR::sql("select * from parking_temp_2015_sql
                            where `Summons Number` is not null")

## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(parking_2015, "parking_2015_sql")

### Checking invalid values for years 2016

grouped_years_2016 <- SparkR::sql("select YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as Year, 
                                  count(*) as no_of_entries
                                  from cleaned_master_frame_2016_sql
                                  group by YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)))
                                  order by count(*) desc")

grouped_years_df_2016 <- collect(grouped_years_2016)

grouped_years_df_2016   

# As we can see, there are many years other than year 2015 and 2016. However,
# We are only interested in the entries between July-1-2015 to June-30-2016. Rest all donot count
# into Fiscal Year 2016, as per above mentioned NYC FAQ link . Hence that needs to be cleaned.

### Counting the entries which donot lie in the fiscal range defined above

invalid_entries_2016 <- SparkR::sql("select count(*) as invalid_entries_2016
                                    from cleaned_master_frame_2016_sql
                                    WHERE TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)) 
                                    NOT BETWEEN '2015-07-01' AND '2016-06-30'")

num_invalid_entries_2016 <- collect(invalid_entries_2016)
num_invalid_entries_2016  #= 230005 

### We need to remove these invalid entries. Also, the total_entries_2016 = 10626899 and
### num_invalid_entries_2016 = 230005, which is 230005/10626899 = 2.16% of the total dataset for 2016.
### Hence, it is fairly reasonable amount to remove, and wont affect the analysis of dataset much.

# Keeping only the entries whose Issue data lie in the range of Fiscal Year 2016.
parking_temp_2016 <- SparkR::sql("select *
                                 from cleaned_master_frame_2016_sql
                                 WHERE TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)) 
                                 BETWEEN '2015-07-01' AND '2016-06-30'")

## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(parking_temp_2016, "parking_temp_2016_sql")


# Dropping Null values in Summons Number column if any
parking_2016 <- SparkR::sql("select * from parking_temp_2016_sql
                            where `Summons Number` is not null")

## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(parking_2016, "parking_2016_sql")

### Checking invalid values for years 2017

grouped_years_2017 <- SparkR::sql("select YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as Year, 
                                  count(*) as no_of_entries
                                  from cleaned_master_frame_2017_sql
                                  group by YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)))
                                  order by count(*) desc")

grouped_years_df_2017 <- collect(grouped_years_2017)

grouped_years_df_2017   

# As we can see, there are many years other than year 2016 and 2017. However,
# We are only interested in the entries between July-1-2016 to June-30-2017. Rest all donot count
# into Fiscal Year 2017, as per above mentioned NYC FAQ link . Hence that needs to be cleaned.

### Counting the entries which donot lie in the fiscal range defined above

invalid_entries_2017 <- SparkR::sql("select count(*) as invalid_entries_2017
                                    from cleaned_master_frame_2017_sql
                                    WHERE TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)) 
                                    NOT BETWEEN '2016-07-01' AND '2017-06-30'")

num_invalid_entries_2017 <- collect(invalid_entries_2017)
num_invalid_entries_2017  #= 263465 

### We need to remove these invalid entries. Also, the total_entries_2017 = 10803028 and
### num_invalid_entries_2016 = 263465, which is 263465/10803028 = 2.43% of the total dataset for 2017.
### Hence, it is fairly reasonable amount to remove, and wont affect the analysis of dataset much.

# Keeping only the entries whose Issue data lie in the range of Fiscal Year 2017.
parking_temp_2017 <- SparkR::sql("select *
                                 from cleaned_master_frame_2017_sql
                                 WHERE TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)) 
                                 BETWEEN '2016-07-01' AND '2017-06-30'")

## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(parking_temp_2017, "parking_temp_2017_sql")


# Dropping Null values in Summons Number column if any
parking_2017 <- SparkR::sql("select * from parking_temp_2017_sql
                            where `Summons Number` is not null")

## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(parking_2017, "parking_2017_sql")


# Now we are ready to run our queries, since we have done the basic quality checks,
# and can move on to find further quality issues and insights from the above cleaned datasets.


######################################## Examine Data ###############################################

#####################################################################################################

### 1) Find total number of tickets for each year.

# Since we have already removed the duplicated values, null values, and invalid entries 
# hence all the remaining entries are valid and consists of unique `Summons Number`. 
# Thus, the total tickets for each year can be straightaway counted by counting 
# the num_of_rows in the cleaned dataset created above.

# total_tickets_2015 = 10598035
total_tickets_2015 <- nrow(parking_2015)
total_tickets_2015

# total_tickets_2016 = 10396894
total_tickets_2016 <- nrow(parking_2016)
total_tickets_2016

# total_tickets_2017 = 10539563
total_tickets_2017 <- nrow(parking_2017)
total_tickets_2017


# Comparing the outputs of each fiscal year through plots
vec_years <- c('2015', '2016', '2017')
vec_tickets <- c(total_tickets_2015, total_tickets_2016, total_tickets_2017)

plot_tickets <- ggplot() + aes(x = vec_years, y = vec_tickets) + geom_col(width = 0.5) + geom_text(aes(label = vec_tickets), vjust = -0.25) +
                xlab('Fiscal Years') + ylab('Tickets Issued') + ggtitle('Total Tickets Issued for each Fiscal Year') + 
                theme_grey()

plot_tickets

################################################################################################################################

### 2) Find out how many unique states the cars which got parking tickets came from.

distVals_state_2015 <- collect(distinct(select(parking_2015, parking_2015$`Registration State`)))

distVals_state_2016 <- collect(distinct(select(parking_2016, parking_2016$`Registration State`)))

distVals_state_2017 <- collect(distinct(select(parking_2017, parking_2017$`Registration State`)))

# unique states form parking_2015 dataset = 69
distVals_state_2015

# unique states from parking_2016 dataset = 68
distVals_state_2016

# unique states from parking_2017 dataset = 67
distVals_state_2017

# -- As we can see, there is one data entry error in all the three data sets, 
# -- where a state has been entered as 99.So after ignoring that error, 
# -- the Number of distinct state in dataset of various years is equal to:

# 2015 -- 69(Total distinct values) - 1(error) = 68 unique states
num_unique_states_2015 <- nrow(distVals_state_2015) - 1

# 2016 -- 68(Total distinct values) - 1(error) = 67 states
num_unique_states_2016 <- nrow(distVals_state_2016) - 1

# 2017 -- 67(Total distinct values) - 1(error) = 66 states.
num_unique_states_2017 <- nrow(distVals_state_2017) - 1


# Comparing the outputs of each fiscal year through plots
vec_years <- c('2015', '2016', '2017')
vec_unique_states <- c(num_unique_states_2015, num_unique_states_2016, num_unique_states_2017)

plot_states <- ggplot() + aes(x = vec_years, y = vec_unique_states) + geom_col(width = 0.5) + geom_text(aes(label = vec_unique_states), vjust = -0.25) +
               xlab('Fiscal Years') + ylab('Unique States') + ggtitle('Number of Unique States') + 
               theme_grey()
  

plot_states

##########################################################################################################################

### 3) Some parking tickets donot have addresses on them, which is cause for concern. 
###    Find out how many such tickets there are.

### There is no specific column which contains the address. However, as mentioned by the
### TA's on the platform, if any row has all the values missing for the following three columns:
### a) House Number, b) Street Name, c) Intersecting Street, then that ticket should be counted
### as the ticket with a missing address


## Missing Address for Year 2015
missing_address_2015 <- SparkR::sql("select count(*) as num_entries_misssing_address_2015
                                    from parking_2015_sql
                                    where `House Number` is NULL AND
                                    `Street Name` is NULL AND
                                    `Intersecting Street` is NULL")


num_missing_address_2015 <- collect(missing_address_2015)
num_missing_address_2015   #3696


## Missing Address for Year 2016
missing_address_2016 <- SparkR::sql("select count(*) as num_entries_misssing_address_2016
                                    from parking_2016_sql
                                    where `House Number` is NULL AND
                                    `Street Name` is NULL AND
                                    `Intersecting Street` is NULL")


num_missing_address_2016 <- collect(missing_address_2016)
num_missing_address_2016   #2640


## Missing Address for Year 2017
missing_address_2017 <- SparkR::sql("select count(*) as num_entries_misssing_address_2017
                                    from parking_2017_sql
                                    where `House Number` is NULL AND
                                    `Street Name` is NULL AND
                                    `Intersecting Street` is NULL")


num_missing_address_2017 <- collect(missing_address_2017)
num_missing_address_2017   #2418


# Comparing the outputs of each fiscal year through plots
vec_years <- c('2015', '2016', '2017')

vec_missing_address<- c(num_missing_address_2015$num_entries_misssing_address_2015, 
                        num_missing_address_2016$num_entries_misssing_address_2016, 
                        num_missing_address_2017$num_entries_misssing_address_2017)


plot_missing_address <- ggplot() + aes(x = vec_years, y = vec_missing_address) + geom_col(width = 0.5) + geom_text(aes(label = vec_missing_address), vjust = -0.25) +
                        xlab('Fiscal Years') + ylab('Tickets Issued') + ggtitle('Number of Tickets with Missing Address') + 
                        theme_grey()

plot_missing_address


######################################## Aggregation tasks ###############################################

###########################################################################################################################

##### 1) How often does each violation code occur?(frequency of violation codes - find the top 5)


### Top 5 "Violation Codes" for year 2015
violation_code_2015 <- summarize(groupBy(parking_2015, parking_2015$`Violation Code`), count = n(parking_2015$`Violation Code`))
violation_code_top5_2015 <- head(arrange(violation_code_2015, desc(violation_code_2015$count)), 5)
violation_code_top5_2015$year <- c(replicate(5, '2015'))
violation_code_top5_2015

# Results:

#   Violation Code   count year
# 1             21 1469228 2015
# 2             38 1305007 2015
# 3             14  908418 2015
# 4             36  747098 2015
# 5             37  735600 2015


### Top 5 "Violation Codes" for year 2016
violation_code_2016 <- summarize(groupBy(parking_2016, parking_2016$`Violation Code`), count = n(parking_2016$`Violation Code`))
violation_code_top5_2016 <- head(arrange(violation_code_2016, desc(violation_code_2016$count)), 5)
violation_code_top5_2016$year <- c(replicate(5, '2016'))
violation_code_top5_2016

# Results: 

#   Violation Code   count year
# 1             21 1497269 2016
# 2             36 1232952 2016
# 3             38 1126835 2016
# 4             14  860045 2016
# 5             37  677805 2016


### Top 5 "Violation Codes" for year 2017
violation_code_2017 <- summarize(groupBy(parking_2017, parking_2017$`Violation Code`), count = n(parking_2017$`Violation Code`))
violation_code_top5_2017 <- head(arrange(violation_code_2017, desc(violation_code_2017$count)), 5)
violation_code_top5_2017$year <- c(replicate(5, '2017'))
violation_code_top5_2017

# Results:

#   Violation Code   count year
# 1             21 1500396 2017
# 2             36 1345237 2017
# 3             38 1050418 2017
# 4             14  880152 2017
# 5             20  609231 2017


# Comparing the outputs of each fiscal year through plots
violation_code_df <- rbind(violation_code_top5_2015, violation_code_top5_2016, violation_code_top5_2017)
violation_code_df

plot_violation_code <- ggplot(violation_code_df, aes(factor(`Violation Code`), count)) + geom_col() + theme_bw() + 
                       facet_wrap(~year) + xlab('Violation Codes') + ylab('Tickets Issued') + 
                       ggtitle('Top 5 Violation Codes for each Fiscal Year')

plot_violation_code

#############################################################################################################################

##### 2) How often does each vehicle body type get a parking ticket? How about the vehicle make? 
#####    (find the top 5 for both)


### Top 5 "Vehicle Body Type" for year 2015
vehicle_body_type_2015 <- summarize(groupBy(parking_2015, parking_2015$`Vehicle Body Type`), count = n(parking_2015$`Vehicle Body Type`))
vehicle_body_type_top5_2015 <- head(arrange(vehicle_body_type_2015, desc(vehicle_body_type_2015$count)), 5)
vehicle_body_type_top5_2015$year <- c(replicate(5, '2015'))
vehicle_body_type_top5_2015


# Results:

#   Vehicle Body Type   count year
# 1              SUBN 3341110 2015
# 2              4DSD 3001810 2015
# 3               VAN 1570227 2015
# 4              DELV  822040 2015
# 5               SDN  428571 2015


### Top 5 "Vehicle Body Type" for year 2016
vehicle_body_type_2016 <- summarize(groupBy(parking_2016, parking_2016$`Vehicle Body Type`), count = n(parking_2016$`Vehicle Body Type`))
vehicle_body_type_top5_2016 <- head(arrange(vehicle_body_type_2016, desc(vehicle_body_type_2016$count)), 5)
vehicle_body_type_top5_2016$year <- c(replicate(5, '2016'))
vehicle_body_type_top5_2016


# Results:

#   Vehicle Body Type   count year
# 1              SUBN 3393838 2016
# 2              4DSD 2936729 2016
# 3               VAN 1489924 2016
# 4              DELV  738747 2016
# 5               SDN  401750 2016


### Top 5 "Vehicle Body Type" for year 2017
vehicle_body_type_2017 <- summarize(groupBy(parking_2017, parking_2017$`Vehicle Body Type`), count = n(parking_2017$`Vehicle Body Type`))
vehicle_body_type_top5_2017 <- head(arrange(vehicle_body_type_2017, desc(vehicle_body_type_2017$count)), 5)
vehicle_body_type_top5_2017$year <- c(replicate(5, '2017'))
vehicle_body_type_top5_2017


# Results:

#   Vehicle Body Type   count year
# 1              SUBN 3632003 2017
# 2              4DSD 3017372 2017
# 3               VAN 1384121 2017
# 4              DELV  672123 2017
# 5               SDN  414984 2017


# Comparing the outputs of each fiscal year through plots
vehicle_body_type_df <- rbind(vehicle_body_type_top5_2015, vehicle_body_type_top5_2016, vehicle_body_type_top5_2017)
vehicle_body_type_df

plot_vehicle_body_type <- ggplot(vehicle_body_type_df, aes(factor(`Vehicle Body Type`), count)) + geom_col() + theme_bw() + 
                          facet_wrap(~year) + xlab('Vehicle Body Type') + ylab('Tickets Issued') + 
                          ggtitle('Top 5 Vehicle Bodies which violated the rules maximum times')
  
plot_vehicle_body_type



### Top 5 "Vehicle Make" for year 2015
vehicle_make_2015 <- summarize(groupBy(parking_2015, parking_2015$`Vehicle Make`), count = n(parking_2015$`Vehicle Make`))
vehicle_make_top5_2015 <- head(arrange(vehicle_make_2015, desc(vehicle_make_2015$count)), 5)
vehicle_make_top5_2015$year <- c(replicate(5, '2015'))
vehicle_make_top5_2015


# Results:

#   Vehicle Make   count year
# 1         FORD 1373157 2015
# 2        TOYOT 1082206 2015
# 3        HONDA  982130 2015
# 4        CHEVR  811659 2015
# 5        NISSA  805572 2015


### Top 5 "Vehicle Make" for year 2016
vehicle_make_2016 <- summarize(groupBy(parking_2016, parking_2016$`Vehicle Make`), count = n(parking_2016$`Vehicle Make`))
vehicle_make_top5_2016 <- head(arrange(vehicle_make_2016, desc(vehicle_make_2016$count)), 5)
vehicle_make_top5_2016$year <- c(replicate(5, '2016'))
vehicle_make_top5_2016


# Results:

#   Vehicle Make   count year
# 1         FORD 1297363 2016
# 2        TOYOT 1128909 2016
# 3        HONDA  991735 2016
# 4        NISSA  815963 2016
# 5        CHEVR  743416 2016


### Top 5 "Vehicle Make" for year 2017
vehicle_make_2017 <- summarize(groupBy(parking_2017, parking_2017$`Vehicle Make`), count = n(parking_2017$`Vehicle Make`))
vehicle_make_top5_2017 <- head(arrange(vehicle_make_2017, desc(vehicle_make_2017$count)), 5)
vehicle_make_top5_2017$year <- c(replicate(5, '2017'))
vehicle_make_top5_2017


# Results:

#   Vehicle Make   count year
# 1         FORD 1250777 2017
# 2        TOYOT 1179265 2017
# 3        HONDA 1052006 2017
# 4        NISSA  895225 2017
# 5        CHEVR  698024 2017


# Comparing the outputs of each fiscal year through plots
vehicle_make_top5_df <- rbind(vehicle_make_top5_2015, vehicle_make_top5_2016, vehicle_make_top5_2017)
vehicle_make_top5_df

plot_vehicle_make <- ggplot(vehicle_make_top5_df, aes(factor(`Vehicle Make`), count)) + geom_col() + theme_bw() + 
                          facet_wrap(~year) + xlab('Vehicle Make') + ylab('Tickets Issued') + 
                          ggtitle('Top 5 Vehicle Making Companies whose cars violated rules maximum times') + 
                          theme(axis.text.x = element_text(angle = 90, vjust = 0.5))

plot_vehicle_make


#############################################################################################################################

###3. A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of:

##3.a Violating Precincts (this is the precinct of the zone where the violation occurred)

# 2015 - Violating Precincts - this is the precinct of the zone where the violation occurred)
violationPrecinct_2015 <- summarize(groupBy(parking_2015, parking_2015$`Violation Precinct`), count = n(parking_2015$`Violation Precinct`))
violationPrecinct_df_2015 <- head(arrange(violationPrecinct_2015, desc(violationPrecinct_2015$count)),5)
violationPrecinct_df_2015$year <- c(replicate(5, '2015'))
violationPrecinct_df_2015

# RESULT

#   Violation Precinct   count year
# 1                  0 1455166 2015
# 2                 19  550797 2015
# 3                 18  393802 2015
# 4                 14  377750 2015
# 5                  1  302737 2015


# 2016 - Violating Precincts -  the precinct of the zone where the violation occurred) > 0
violationPrecinct_2016 <- summarize(groupBy(parking_2016, parking_2016$`Violation Precinct`), count = n(parking_2016$`Violation Precinct`))
violationPrecinct_df_2016 <- head(arrange(violationPrecinct_2016, desc(violationPrecinct_2016$count)),5)
violationPrecinct_df_2016$year <- c(replicate(5, '2016'))
violationPrecinct_df_2016


# RESULT

#   Violation Precinct   count year
# 1                  0 1807139 2016
# 2                 19  545669 2016
# 3                 18  325559 2016
# 4                 14  318193 2016
# 5                  1  299074 2016


#2017 - Violating Precincts -  is the precinct of the zone where the violation occurred > 0
violationPrecinct_2017 <- summarize(groupBy(parking_2017, parking_2017$`Violation Precinct`), count = n(parking_2017$`Violation Precinct`))
violationPrecinct_df_2017 <- head(arrange(violationPrecinct_2017, desc(violationPrecinct_2017$count)),5)
violationPrecinct_df_2017$year <- c(replicate(5, '2017'))
violationPrecinct_df_2017


# RESULT

#   Violation Precinct   count year
# 1                  0 1950083 2017
# 2                 19  528317 2017
# 3                 14  347736 2017
# 4                  1  326961 2017
# 5                 18  302008 2017



# Comparing the outputs of each fiscal year through plots
violationPrecinct_df <- rbind(violationPrecinct_df_2015, violationPrecinct_df_2016, violationPrecinct_df_2017)
violationPrecinct_df

plot_violationPrecinct <- ggplot(violationPrecinct_df, aes(factor(`Violation Precinct`), count)) + geom_col() + theme_bw() + 
                          facet_wrap(~year) + xlab('Violation Precinct') + ylab('Tickets Issued') + 
                          ggtitle('Top 5 Violation Precincts for each Fiscal Year')

plot_violationPrecinct




## 3.b Issuing Precincts (this is the precinct that issued the ticket)


# 2015 Issuing Precincts
issuerPrecinct_2015 <- summarize(groupBy(parking_2015, parking_2015$`Issuer Precinct`), count = n(parking_2015$`Issuer Precinct`))
issuerPrecinct_df_2015 <- head(arrange(issuerPrecinct_2015, desc(issuerPrecinct_2015$count)), 5)
issuerPrecinct_df_2015$year <- c(replicate(5, '2015'))
issuerPrecinct_df_2015

# RESULT

#   Issuer Precinct   count year
# 1               0 1648671 2015
# 2              19  536627 2015
# 3              18  384863 2015
# 4              14  363734 2015
# 5               1  293942 2015


# 2016 Issuing Precincts
issuerPrecinct_2016 <- summarize(groupBy(parking_2016, parking_2016$`Issuer Precinct`), count = n(parking_2016$`Issuer Precinct`))
issuerPrecinct_df_2016 <- head(arrange(issuerPrecinct_2016, desc(issuerPrecinct_2016$count)), 5)
issuerPrecinct_df_2016$year <- c(replicate(5, '2016'))
issuerPrecinct_df_2016

# RESULT

#   Issuer Precinct   count year
# 1               0 2067219 2016
# 2              19  532298 2016
# 3              18  317451 2016
# 4              14  309727 2016
# 5               1  290472 2016


# 2017 Issuing Precincts
issuerPrecinct_2017 <- summarize(groupBy(parking_2017, parking_2017$`Issuer Precinct`), count = n(parking_2017$`Issuer Precinct`))
issuerPrecinct_df_2017 <- head(arrange(issuerPrecinct_2017, desc(issuerPrecinct_2017$count)), 5)
issuerPrecinct_df_2017$year <- c(replicate(5, '2017'))
issuerPrecinct_df_2017

# RESULT

#   Issuer Precinct   count year
# 1               0 2255086 2017
# 2              19  514786 2017
# 3              14  340862 2017
# 4               1  316776 2017
# 5              18  292237 2017


# Comparing the outputs of each fiscal year through plots
issuerPrecinct_df <- rbind(issuerPrecinct_df_2015, issuerPrecinct_df_2016, issuerPrecinct_df_2017)
issuerPrecinct_df

plot_issuerPrecinct <- ggplot(issuerPrecinct_df, aes(factor(`Issuer Precinct`), count)) + geom_col() + theme_bw() + 
                       facet_wrap(~year) + xlab('Issuer Precinct') + ylab('Tickets Issued') + 
                       ggtitle('Top 5 Issuer Precinct for each Fiscal Year')

plot_issuerPrecinct


#############################################################################################################################

### 4. Find the violation code frequency across 3 precincts which have issued the most number of tickets - 
### do these precinct zones have an exceptionally high frequency of certain violation codes? 
### Are these codes common across precincts?



#### 2015 ###################################################################################

## The 3 precincts which have issued the most number of tickets for year 2015 are:

#     Issuer_Precinct_2015  no_of_tickets
# 1                    0     1648671
# 2                   19      536627
# 3                   18      384863


# Finding violation code frequency across each of these precincts:

## Precinct1 = 0

issuerPrecinct0 <- filter(parking_2015, parking_2015$`Issuer Precinct` == 0)
ViolationCodeissuerPrecinct0 <- summarize(groupBy(issuerPrecinct0, issuerPrecinct0$`Violation Code`), count = n(issuerPrecinct0$`Violation Code`))
VCF_2015_Precinct0<-head(arrange(ViolationCodeissuerPrecinct0, desc(ViolationCodeissuerPrecinct0$count)), 10)
VCF_2015_Precinct0$year<-c(replicate(10,'2015'))
VCF_2015_Precinct0$Precinct<-c(replicate(10,'0'))
VCF_2015_Precinct0

# RESULT

#    Violation Code  count year Precinct
# 1              36 747098 2015        0
# 2               7 567951 2015        0
# 3              21 173191 2015        0
# 4               5 127153 2015        0
# 5              66   4703 2015        0
# 6              14   4007 2015        0
# 7              17   2791 2015        0
# 8              20   2647 2015        0
# 9              46   2103 2015        0
# 10             78   1641 2015        0


## Precinct2 = 19

issuerPrecinct19 <- filter(parking_2015, parking_2015$`Issuer Precinct` == 19)
ViolationCodeissuerPrecinct19 <- summarize(groupBy(issuerPrecinct19, issuerPrecinct19$`Violation Code`), count = n(issuerPrecinct19$`Violation Code`))
VCF_2015_Precinct19<-head(arrange(ViolationCodeissuerPrecinct19, desc(ViolationCodeissuerPrecinct19$count)), 10)
VCF_2015_Precinct19$year<-c(replicate(10,'2015'))
VCF_2015_Precinct19$Precinct<-c(replicate(10,'19'))
VCF_2015_Precinct19


# RESULT

#    Violation Code count year Precinct
# 1              38 89102 2015       19
# 2              37 78716 2015       19
# 3              14 59915 2015       19
# 4              16 55762 2015       19
# 5              21 55296 2015       19
# 6              46 43114 2015       19
# 7              20 31740 2015       19
# 8              40 27409 2015       19
# 9              71 16817 2015       19
# 10             19 14675 2015       19


## Precinct3 = 18

issuerPrecinct18 <- filter(parking_2015, parking_2015$`Issuer Precinct` == 18)
ViolationCodeissuerPrecinct18 <- summarize(groupBy(issuerPrecinct18, issuerPrecinct18$`Violation Code`), count = n(issuerPrecinct18$`Violation Code`))
VCF_2015_Precinct18<-head(arrange(ViolationCodeissuerPrecinct18, desc(ViolationCodeissuerPrecinct18$count)), 10)
VCF_2015_Precinct18$year<-c(replicate(10,'2015'))
VCF_2015_Precinct18$Precinct<-c(replicate(10,'18'))
VCF_2015_Precinct18


# RESULT

#    Violation Code  count year Precinct
# 1              14 119078 2015       18
# 2              69  56436 2015       18
# 3              31  30030 2015       18
# 4              47  28724 2015       18
# 5              42  19522 2015       18
# 6              38  18334 2015       18
# 7              46  16268 2015       18
# 8              84   9802 2015       18
# 9              19   8507 2015       18
# 10             37   8318 2015       18


#--------------->2015 Analysis : The violation code common among top 3 Issuer Precincts of year 2015 are: 14 and 46
#Combining 2015 Precinct's data together
ViolationCodeFreq_2015<-rbind(VCF_2015_Precinct0,VCF_2015_Precinct18,VCF_2015_Precinct19)
ViolationCodeFreq_2015

#Comparing the outputs of each Precincts through plots
plot_ViolationCodeFreq_2015 <- ggplot(ViolationCodeFreq_2015, aes(factor(`Violation Code`), count)) + geom_col() + theme_bw() + 
                               facet_wrap(~Precinct) + xlab('Violation Code') + ylab('Tickets Issued') + 
                               ggtitle('Year 2015 : Top 10 violation code for top 3 precincts') + theme(axis.text.x = element_text(angle = 90, vjust = 0.5))
plot_ViolationCodeFreq_2015
  
#### 2016 ###################################################################################

## The 3 precincts which have issued the most number of tickets for year 2016 are:

#     Issuer_Precinct_2016  no_of_tickets
# 1                    0     2067219
# 2                   19      532298
# 3                   18      317451

# Finding violation code frequency across each of these precincts:

## Precinct1 = 0

issuerPrecinct0 <- filter(parking_2016, parking_2016$`Issuer Precinct` == 0)
ViolationCodeissuerPrecinct0 <- summarize(groupBy(issuerPrecinct0, issuerPrecinct0$`Violation Code`), count = n(issuerPrecinct0$`Violation Code`))
VCF_2016_Precinct0<-head(arrange(ViolationCodeissuerPrecinct0, desc(ViolationCodeissuerPrecinct0$count)), 10)
VCF_2016_Precinct0$year<-c(replicate(10,'2016'))
VCF_2016_Precinct0$Precinct<-c(replicate(10,'0'))
VCF_2016_Precinct0

# RESULT

#    Violation Code   count year Precinct
# 1              36 1232951 2016        0
# 2               7  457871 2016        0
# 3              21  226687 2016        0
# 4               5  106617 2016        0
# 5              66    7275 2016        0
# 6              14    5800 2016        0
# 7              85    3667 2016        0
# 8              17    2968 2016        0
# 9              20    2826 2016        0
# 10             78    2108 2016        0


## Precinct2 = 19

issuerPrecinct19 <- filter(parking_2016, parking_2016$`Issuer Precinct` == 19)
ViolationCodeissuerPrecinct19 <- summarize(groupBy(issuerPrecinct19, issuerPrecinct19$`Violation Code`), count = n(issuerPrecinct19$`Violation Code`))
VCF_2016_Precinct19<-head(arrange(ViolationCodeissuerPrecinct19, desc(ViolationCodeissuerPrecinct19$count)), 10)
VCF_2016_Precinct19$year<-c(replicate(10,'2016'))
VCF_2016_Precinct19$Precinct<-c(replicate(10,'19'))
VCF_2016_Precinct19

# RESULT

#    Violation Code count year Precinct
# 1              38 76178 2016       19
# 2              37 74758 2016       19
# 3              46 71509 2016       19
# 4              14 60856 2016       19
# 5              21 57601 2016       19
# 6              16 51622 2016       19
# 7              20 28914 2016       19
# 8              40 21517 2016       19
# 9              71 15060 2016       19
# 10             19 13426 2016       19


## Precinct3 = 18

issuerPrecinct18 <- filter(parking_2016, parking_2016$`Issuer Precinct` == 18)
ViolationCodeissuerPrecinct18 <- summarize(groupBy(issuerPrecinct18, issuerPrecinct18$`Violation Code`), count = n(issuerPrecinct18$`Violation Code`))
VCF_2016_Precinct18<-head(arrange(ViolationCodeissuerPrecinct18, desc(ViolationCodeissuerPrecinct18$count)), 10)
VCF_2016_Precinct18$year<-c(replicate(10,'2016'))
VCF_2016_Precinct18$Precinct<-c(replicate(10,'18'))
VCF_2016_Precinct18

# RESULT

#    Violation Code count year Precinct
# 1              14 98160 2016       18
# 2              69 47129 2016       18
# 3              47 23618 2016       18
# 4              31 22413 2016       18
# 5              42 17416 2016       18
# 6              46 14277 2016       18
# 7              38 13981 2016       18
# 8              84  8886 2016       18
# 9              19  7350 2016       18
# 10             37  7034 2016       18



#--------------->2016 Analysis : The violation code common among top 3 Issuer Precincts of year 2016 are: 14

#Combining 2016 Precinct's data together
ViolationCodeFreq_2016<-rbind(VCF_2016_Precinct0,VCF_2016_Precinct18,VCF_2016_Precinct19)
ViolationCodeFreq_2016

#Comparing the outputs of each Precincts through plots
plot_ViolationCodeFreq_2016 <- ggplot(ViolationCodeFreq_2016, aes(factor(`Violation Code`), count)) + geom_col() + theme_bw() + 
                               facet_wrap(~Precinct) + xlab('Violation Code') + ylab('Tickets Issued') + 
                               ggtitle('Year 2016 : Top 10 violation code for top 3 precincts') + theme(axis.text.x = element_text(angle = 90, vjust = 0.5))

plot_ViolationCodeFreq_2016

#### 2017 #############

## The 3 precincts which have issued the most number of tickets for year 2017 are:

#   Issuer Precinct   count
# 1               0 2255086
# 2              19  514786
# 3              14  340862

# Finding violation code frequency across each of these precincts:

## Precinct1 = 0

issuerPrecinct0 <- filter(parking_2017, parking_2017$`Issuer Precinct` == 0)
ViolationCodeissuerPrecinct0 <- summarize(groupBy(issuerPrecinct0, issuerPrecinct0$`Violation Code`), count = n(issuerPrecinct0$`Violation Code`))
VCF_2017_Precinct0<-head(arrange(ViolationCodeissuerPrecinct0, desc(ViolationCodeissuerPrecinct0$count)), 10)
VCF_2017_Precinct0$year<-c(replicate(10,'2017'))
VCF_2017_Precinct0$Precinct<-c(replicate(10,'0'))
VCF_2017_Precinct0


# RESULT

#    Violation Code   count year Precinct
# 1              36 1345237 2017        0
# 2               7  464690 2017        0
# 3              21  258771 2017        0
# 4               5  130963 2017        0
# 5              66    9281 2017        0
# 6              14    6848 2017        0
# 7              78    6704 2017        0
# 8              20    4971 2017        0
# 9              17    3410 2017        0
# 10             40    2663 2017        0


## Precinct2 = 19

issuerPrecinct19 <- filter(parking_2017, parking_2017$`Issuer Precinct` == 19)
ViolationCodeissuerPrecinct19 <- summarize(groupBy(issuerPrecinct19, issuerPrecinct19$`Violation Code`), count = n(issuerPrecinct19$`Violation Code`))
VCF_2017_Precinct19<-head(arrange(ViolationCodeissuerPrecinct19, desc(ViolationCodeissuerPrecinct19$count)), 10)
VCF_2017_Precinct19$year<-c(replicate(10,'2017'))
VCF_2017_Precinct19$Precinct<-c(replicate(10,'19'))
VCF_2017_Precinct19


# RESULT

#    Violation Code count year Precinct
# 1              46 84789 2017       19
# 2              38 71631 2017       19
# 3              37 71592 2017       19
# 4              14 56873 2017       19
# 5              21 54033 2017       19
# 6              16 30722 2017       19
# 7              20 27032 2017       19
# 8              40 21296 2017       19
# 9              71 15048 2017       19
# 10             19 12744 2017       19


## Precinct3 = 14

issuerPrecinct14 <- filter(parking_2017, parking_2017$`Issuer Precinct` == 14)
ViolationCodeissuerPrecinct14 <- summarize(groupBy(issuerPrecinct14, issuerPrecinct14$`Violation Code`), count = n(issuerPrecinct14$`Violation Code`))
VCF_2017_Precinct14<-head(arrange(ViolationCodeissuerPrecinct14, desc(ViolationCodeissuerPrecinct14$count)), 10)
VCF_2017_Precinct14$year<-c(replicate(10,'2017'))
VCF_2017_Precinct14$Precinct<-c(replicate(10,'14'))
VCF_2017_Precinct14

# RESULT

#    Violation Code count year Precinct
# 1              14 73007 2017       14
# 2              69 57316 2017       14
# 3              31 39430 2017       14
# 4              47 30200 2017       14
# 5              42 20402 2017       14
# 6              46 13040 2017       14
# 7              84 11000 2017       14
# 8              19 10952 2017       14
# 9              82  8785 2017       14
# 10             17  6072 2017       14


#--------------->2017 Analysis : The violation code common among top 3 Issuer Precincts of year 2017 are: 14

#Combining 2015 Precinct's data together
ViolationCodeFreq_2017<-rbind(VCF_2017_Precinct0,VCF_2017_Precinct14,VCF_2017_Precinct19)
ViolationCodeFreq_2017

#Comparing the outputs of each Precincts through plots
plot_ViolationCodeFreq_2017 <- ggplot(ViolationCodeFreq_2017, aes(factor(`Violation Code`), count)) + geom_col() + theme_bw() + 
                               facet_wrap(~Precinct) + xlab('Violation Code') + ylab('Tickets Issued') + 
                               ggtitle('Year 2017 : Top 10 violation code for top 3 precincts') + theme(axis.text.x = element_text(angle = 90, vjust = 0.5))

plot_ViolationCodeFreq_2017

###########Comparing the outputs of each year through plot#############
ViolationCodeFreq<-rbind(ViolationCodeFreq_2015,ViolationCodeFreq_2016,ViolationCodeFreq_2017)
ViolationCodeFreq


plot_ViolationCodeFreq<- ggplot(ViolationCodeFreq, aes(factor(`Violation Code`), count)) + geom_col() + 
                         facet_grid(year~Precinct)+ xlab('Violation Code') + ylab('Tickets Issued') +
                         ggtitle('Yearwise Top 10 violation code for top 3 precincts') + theme(axis.text.x = element_text(angle = 90, vjust = 0.5))

plot_ViolationCodeFreq


############################################################################################################################

###### 5) You'd want to find out the properties of parking violations across different times of 
#         the day:

######    A) The Violation Time field is specified in a strange format. 
######       Find a way to make this into a time attribute that you can use to divide into groups.

### Converting Violation Time field into a attribute which can be used to divide into groups later.

# Year: 2015

Violation_Time_Attribute_2015 <- SparkR::sql("select `Violation Code` as Violation_Code,
                                             `Violation Time` as Violation_Time_2015,
                                             CAST(substr(`Violation Time`,1,2) as INT) as Hour,
                                             substr(`Violation Time`, 5, 5) as AM_PM
                                             from parking_2015_sql")

# Since we have used substr method, there is a possibility of getting blank strings ('') introduced
# in the newly derived columns, which will be a data entry. Hence, converting these data entry
# by replacing them with NA's so that all NA's can be dropped at once, later.

# Converting blank strings ('') into NA's
Violation_Time_Attribute_2015$Hour <- ifelse(Violation_Time_Attribute_2015$Hour == '', NA, Violation_Time_Attribute_2015$Hour)
Violation_Time_Attribute_2015$AM_PM <- ifelse(Violation_Time_Attribute_2015$AM_PM == '', NA, Violation_Time_Attribute_2015$AM_PM)


## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(Violation_Time_Attribute_2015, "Violation_Time_Attribute_2015_sql")


# Checking the structure of Data Frame
str(Violation_Time_Attribute_2015)

# Output:

# $ Violation_Code     : int 20 70 20 74 20 14
# $ Violation_Time_2015: chr "0953A" "0520P" "0545P" "0443A" "0327A" "0237A"
# $ Hour               : int 9 5 5 4 3 2
# $ AM_PM               : chr "A" "P" "P" "A" "A" "A"


# Year: 2016

Violation_Time_Attribute_2016 <- SparkR::sql("select `Violation Code` as Violation_Code,
                                             `Violation Time` as Violation_Time_2016,
                                             CAST(substr(`Violation Time`,1,2) as INT) as Hour,
                                             substr(`Violation Time`, 5, 5) as AM_PM
                                             from parking_2016_sql")


# Since we have used substr method, there is a possibility of getting blank strings ('') introduced
# in the newly derived columns, which will be a data entry. Hence, converting these data entry
# by replacing them with NA's so that all NA's can be dropped at once, later.

# Converting blank strings ('') into NA's
Violation_Time_Attribute_2016$Hour <- ifelse(Violation_Time_Attribute_2016$Hour == '', NA, Violation_Time_Attribute_2016$Hour)
Violation_Time_Attribute_2016$AM_PM <- ifelse(Violation_Time_Attribute_2016$AM_PM == '', NA, Violation_Time_Attribute_2016$AM_PM)

## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(Violation_Time_Attribute_2016, "Violation_Time_Attribute_2016_sql")

# Checking the structure of Data Frame
str(Violation_Time_Attribute_2016)

# Output:

# $ Violation_Code     : int 24 20 14 17 68 46
# $ Violation_Time_2016: chr "1103A" "1045A" "0711P" "0859P" "0923P" "0958P"
# $ Hour               : int 11 10 7 8 9 9
# $ AM_PM               : chr "A" "A" "P" "P" "P" "P"


# Year: 2017

Violation_Time_Attribute_2017 <- SparkR::sql("select `Violation Code` as Violation_Code,
                                             `Violation Time` as Violation_Time_2017,
                                             CAST(substr(`Violation Time`,1,2) as INT) as Hour,
                                             substr(`Violation Time`, 5, 5) as AM_PM
                                             from parking_2017_sql")


# Since we have used substr method, there is a possibility of getting blank strings ('') introduced
# in the newly derived columns, which will be a data entry. Hence, converting these data entry
# by replacing them with NA's so that all NA's can be dropped at once, later.

# Converting blank strings ('') into NA's
Violation_Time_Attribute_2017$Hour <- ifelse(Violation_Time_Attribute_2017$Hour == '', NA, Violation_Time_Attribute_2017$Hour)
Violation_Time_Attribute_2017$AM_PM <- ifelse(Violation_Time_Attribute_2017$AM_PM == '', NA, Violation_Time_Attribute_2017$AM_PM)

## Creating a lazily evaluated view for Spark SQL
createOrReplaceTempView(Violation_Time_Attribute_2017, "Violation_Time_Attribute_2017_sql")


# Checking the structure of Data Frame
str(Violation_Time_Attribute_2017)

# Output:
# $ Violation_Code     : int 20 70 17 74 21 20
# $ Violation_Time_2017: chr "0407P" "1119A" "0751A" "1105A" "0423A" "0616A"
# $ Hour               : int 4 11 7 11 4 6
# $ AM_PM               : chr "P" "A" "A" "A" "A" "A"



###### B) Find a way to deal with missing values, if any.

### Year 2015:

missing_violation_time_2015 <- SparkR::sql("select count(*) 
                                           from Violation_Time_Attribute_2015_sql
                                           where Hour is NULL or AM_PM is NULL")

num_missing_violation_time_2015 <- collect(missing_violation_time_2015)

num_missing_violation_time_2015 # 1456

### The total num of parking tickets for year 2015 = 10598035 (As calculated earlier).
### And num of tickets wit missing violation time for year 2015 = 1456 
### which equals to (1456/10598035) 0.01% of the dataset. Now, for such a less amount of dataset,
### it is more logical to drop the entries which doesnot have a violation time entry, instead
### of exaggerating the dataset by some random or calculated interpretation. Hence, we will
### create a new dataframe by dropping the rows with Null values of violation time.

Violation_Time_v1_2015 <- SparkR::sql("select * from Violation_Time_Attribute_2015_sql
                                      where Hour is not NULL AND AM_PM is not NULL")

createOrReplaceTempView(Violation_Time_v1_2015, "Violation_Time_v1_2015_sql")


### Year 2016:

missing_violation_time_2016 <- SparkR::sql("select count(*) 
                                           from Violation_Time_Attribute_2016_sql
                                           where Hour is NULL or AM_PM is NULL")

num_missing_violation_time_2016 <- collect(missing_violation_time_2016)

num_missing_violation_time_2016 # 726

### The total num of parking tickets for year 2016 = 10396894 (As calculated earlier).
### And num of tickets wit missing violation time for year 2016 = 726 
### which equals to (726/10396894) 0.006% of the dataset. Now, for such a less amount of dataset,
### it is more logical to drop the entries which doesnot have a violation time entry, instead
### of exaggerating the dataset by some random or calculated interpretation. Hence, we will
### create a new dataframe by dropping the rows with Null values of violation time.

Violation_Time_v1_2016 <- SparkR::sql("select * from Violation_Time_Attribute_2016_sql
                                      where Hour is not NULL AND AM_PM is not NULL")

createOrReplaceTempView(Violation_Time_v1_2016, "Violation_Time_v1_2016_sql")


### Year 2017:

missing_violation_time_2017 <- SparkR::sql("select count(*) 
                                           from Violation_Time_Attribute_2017_sql
                                           where Hour is NULL or AM_PM is NULL")

num_missing_violation_time_2017 <- collect(missing_violation_time_2017)

num_missing_violation_time_2017 # 72

### The total num of parking tickets for year 2017 = 10539563 (As calculated earlier).
### And num of tickets wit missing violation time for year 2017 = 72 
### which is less than (72/10396894) 0.001% of the dataset. Now, for such a less amount of dataset,
### it is more logical to drop the entries which doesnot have a violation time entry, instead
### of exaggerating the dataset by some random or calculated interpretation. Hence, we will
### create a new dataframe by dropping the rows with Null values of violation time.

Violation_Time_v1_2017 <- SparkR::sql("select * from Violation_Time_Attribute_2017_sql
                                      where Hour is not NULL AND AM_PM is not NULL")

createOrReplaceTempView(Violation_Time_v1_2017, "Violation_Time_v1_2017_sql")


### Verifying the newly derived columns i.e. Hour and AM_PM by looking at
### its distinct values to check for Invalid Values, if any.


# Checking for invalid values in Hour Column
check_Violation_Time_hour <- SparkR::sql("select Hour, count(*) as no_of_tickets from Violation_Time_v1_2016_sql
                                         group by Hour
                                         order by count(*) desc")

check_Violation_Time_hour_df <- collect(check_Violation_Time_hour)

View(check_Violation_Time_hour_df)



# Checking for invalid values in AM_PM Column

check_Violation_Time_AM_PM <- SparkR::sql("select AM_PM, count(*) as no_of_tickets from Violation_Time_v1_2016_sql
                                          group by AM_PM
                                          order by count(*) desc")

check_Violation_Time_AM_PM_df <- collect(check_Violation_Time_AM_PM)

check_Violation_Time_AM_PM_df


# Results:

## V1
# AM_PM no_of_tickets
# 1     A       5347158
# 2     P       5049009
# 3     0             1


# Even after removing the null and empty values, there are still some invalid values They are:
#  i) Hour Column: The values cannot exceed 12, since we are looking at 12-hour format. 
#     Hence all the values above 12 needs to be dropped, since they are invalid
#  ii) AM_PM Column: The AM_PM format should only have two distinct values i.e. AM and PM.
#      Rows with other values should be dropped.

# Dropping all the invalid entries stated above from dataframe of all years

# Year: 2015
Violation_Time_v2_2015 <- SparkR::sql("select *, CONCAT(Hour, ':', AM_PM) as Concatenated_Time from Violation_Time_v1_2015_sql
                                      where Hour <= 12 AND (AM_PM = 'A' or AM_PM = 'P')")


createOrReplaceTempView(Violation_Time_v2_2015, "Violation_Time_v2_2015_sql")



# Year: 2016
Violation_Time_v2_2016 <- SparkR::sql("select *, CONCAT(Hour, ':', AM_PM) as Concatenated_Time from Violation_Time_v1_2016_sql
                                      where Hour <= 12 AND (AM_PM = 'A' or AM_PM = 'P')")


createOrReplaceTempView(Violation_Time_v2_2016, "Violation_Time_v2_2016_sql")


# Year: 2017
Violation_Time_v2_2017 <- SparkR::sql("select *, CONCAT(Hour, ':', AM_PM) as Concatenated_Time from Violation_Time_v1_2017_sql
                                      where Hour <= 12 AND (AM_PM = 'A' or AM_PM = 'P')")


createOrReplaceTempView(Violation_Time_v2_2017, "Violation_Time_v2_2017_sql")



### Concatenating the newly derived columns i.e. Hour and AM_PM and looking at
### its distinct values to check if there is still any error to be corrected

check_Violation_Time <- SparkR::sql("select Concatenated_Time, count(*) as no_of_tickets
                                    from Violation_Time_v2_2016_sql
                                    group by Concatenated_Time")


check_Violation_Time_df <- collect(check_Violation_Time)

View(check_Violation_Time_df)


### We are almost done with preparing the columns to interpret Violation Time.
### However, as we saw above in the data frame "check_Violation_Time_df",
### there are still 4 values in the Violation Time attribute, which are confusing. They are:
### a) 0:P
### b) 12:P
### c) 0:A
### d) 12:A


### Now, as per the meridian time format, 0:A = 12:A = midnight(12AM or OAM), 
### and 0:P = 12:P = Noon(12PM). Also, all the values have enough entries, so we cannot
### drop anyof the two values and keep the other two. Hence, the best way is to convert the 
### values uniformly. For convenience in creation of bins later, we will follow the following conversion:
### We will convert 12:A into 0:A, and 12:P into 0:P. That way,
### it would be less confusing, and easier to interpret and create bins.


# Making the above stated corrections for year 2015:

Violation_Time_v2_2015$Hour <- ifelse(Violation_Time_v2_2015$Concatenated_Time == "12:A",
                                      0, Violation_Time_v2_2015$Hour)


Violation_Time_v2_2015$Concatenated_Time <- ifelse(Violation_Time_v2_2015$Concatenated_Time == "12:A",
                                                   "0:A", Violation_Time_v2_2015$Concatenated_Time)


Violation_Time_v2_2015$Hour <- ifelse(Violation_Time_v2_2015$Concatenated_Time == "12:P",
                                      0, Violation_Time_v2_2015$Hour)


Violation_Time_v2_2015$Concatenated_Time <- ifelse(Violation_Time_v2_2015$Concatenated_Time == "12:P",
                                                   "0:P", Violation_Time_v2_2015$Concatenated_Time)

createOrReplaceTempView(Violation_Time_v2_2015, "Violation_Time_final_2015_sql")



# Making the above stated corrections for year 2016:

Violation_Time_v2_2016$Hour <- ifelse(Violation_Time_v2_2016$Concatenated_Time == "12:A",
                                      0, Violation_Time_v2_2016$Hour)


Violation_Time_v2_2016$Concatenated_Time <- ifelse(Violation_Time_v2_2016$Concatenated_Time == "12:A",
                                                   "0:A", Violation_Time_v2_2016$Concatenated_Time)


Violation_Time_v2_2016$Hour <- ifelse(Violation_Time_v2_2016$Concatenated_Time == "12:P",
                                      0, Violation_Time_v2_2016$Hour)


Violation_Time_v2_2016$Concatenated_Time <- ifelse(Violation_Time_v2_2016$Concatenated_Time == "12:P",
                                                   "0:P", Violation_Time_v2_2016$Concatenated_Time)

createOrReplaceTempView(Violation_Time_v2_2016, "Violation_Time_final_2016_sql")


# Making the above stated corrections for year 2017:

Violation_Time_v2_2017$Hour <- ifelse(Violation_Time_v2_2017$Concatenated_Time == "12:A",
                                      0, Violation_Time_v2_2017$Hour)


Violation_Time_v2_2017$Concatenated_Time <- ifelse(Violation_Time_v2_2017$Concatenated_Time == "12:A",
                                                   "0:A", Violation_Time_v2_2017$Concatenated_Time)


Violation_Time_v2_2017$Hour <- ifelse(Violation_Time_v2_2017$Concatenated_Time == "12:P",
                                      0, Violation_Time_v2_2017$Hour)


Violation_Time_v2_2017$Concatenated_Time <- ifelse(Violation_Time_v2_2017$Concatenated_Time == "12:P",
                                                   "0:P", Violation_Time_v2_2017$Concatenated_Time)

createOrReplaceTempView(Violation_Time_v2_2017, "Violation_Time_final_2017_sql")

# With that done, we now have two derived coulmns, which can help us to get interesting insights 
# realated to Violation Time.


######    C) Divide 24 hours into 6 equal discrete bins of time. 
######       The intervals you choose are at your discretion. 
######       For each of these groups, find the 3 most commonly occurring violations

## Dividing 24 hours into 6 equal discrete bins of time, as follows:

# A) 0 AM (12AM) - 3 AM = MidNight
# B) 4 AM - 7 AM = Dawn
# C) 8 AM - 11AM = Day
# D) 0 PM (12PM) - 3 PM = Noon
# E) 4 PM - 7 PM = Dusk
# F) 8 PM - 11 PM = Night

# Year: 2015

Violation_Time_bins_2015 <- SparkR::sql("select Violation_Code, Violation_Time_2015, Hour, AM_PM, Concatenated_Time, \
                                        case when (Hour >= 0 and Hour <= 3 and AM_PM = 'A') then 'MidNight'\ 
                                        when (Hour >= 4 and Hour <= 7 and AM_PM = 'A') then 'Dawn'\
                                        when (Hour >= 8 and Hour <= 11 and AM_PM = 'A') then 'Day'\
                                        when (Hour >= 0 and Hour <= 3 and AM_PM = 'P') then 'Noon'\
                                        when (Hour >= 4 and Hour <= 7 and AM_PM = 'P') then 'Dusk'\
                                        when (Hour >= 8 and Hour <= 11 and AM_PM = 'P') then 'Night'\ end as Violation_Time_bins
                                        from Violation_Time_final_2015_sql")


createOrReplaceTempView(Violation_Time_bins_2015, "Violation_Time_bins_2015_sql")


head(Violation_Time_bins_2015)


# Results:

#     Violation_Code Violation_Time_2015 Hour AM_PM Concatenated_Time Violation_Time_bins
# 1             20               0953A    9     A               9:A                 Day
# 2             70               0520P    5     P               5:P                Dusk
# 3             20               0545P    5     P               5:P                Dusk
# 4             74               0443A    4     A               4:A                Dawn
# 5             20               0327A    3     A               3:A            MidNight
# 6             14               0237A    2     A               2:A            MidNight

#

# Year: 2016

Violation_Time_bins_2016 <- SparkR::sql("select  Violation_Code, Violation_Time_2016, Hour, AM_PM, Concatenated_Time, \
                                        case when (Hour >= 0 and Hour <= 3 and AM_PM = 'A') then 'MidNight'\ 
                                        when (Hour >= 4 and Hour <= 7 and AM_PM = 'A') then 'Dawn'\
                                        when (Hour >= 8 and Hour <= 11 and AM_PM = 'A') then 'Day'\
                                        when (Hour >= 0 and Hour <= 3 and AM_PM = 'P') then 'Noon'\
                                        when (Hour >= 4 and Hour <= 7 and AM_PM = 'P') then 'Dusk'\
                                        when (Hour >= 8 and Hour <= 11 and AM_PM = 'P') then 'Night'\ end as Violation_Time_bins
                                        from Violation_Time_final_2016_sql")


createOrReplaceTempView(Violation_Time_bins_2016, "Violation_Time_bins_2016_sql")


head(Violation_Time_bins_2016)

# Results:

#     Violation_Code Violation_Time_2016 Hour AM_PM Concatenated_Time Violation_Time_bins
# 1             24               1103A   11     A              11:A                 Day
# 2             20               1045A   10     A              10:A                 Day
# 3             14               0711P    7     P               7:P                Dusk
# 4             17               0859P    8     P               8:P               Night
# 5             68               0923P    9     P               9:P               Night
# 6             46               0958P    9     P               9:P               Night



# Year: 2017

Violation_Time_bins_2017 <- SparkR::sql("select  Violation_Code, Violation_Time_2017, Hour, AM_PM, Concatenated_Time, \
                                        case when (Hour >= 0 and Hour <= 3 and AM_PM = 'A') then 'MidNight'\ 
                                        when (Hour >= 4 and Hour <= 7 and AM_PM = 'A') then 'Dawn'\
                                        when (Hour >= 8 and Hour <= 11 and AM_PM = 'A') then 'Day'\
                                        when (Hour >= 0 and Hour <= 3 and AM_PM = 'P') then 'Noon'\
                                        when (Hour >= 4 and Hour <= 7 and AM_PM = 'P') then 'Dusk'\
                                        when (Hour >= 8 and Hour <= 11 and AM_PM = 'P') then 'Night'\ end as Violation_Time_bins
                                        from Violation_Time_final_2017_sql")


createOrReplaceTempView(Violation_Time_bins_2017, "Violation_Time_bins_2017_sql")


head(Violation_Time_bins_2017)


# Results:

#    Violation_Code Violation_Time_2017 Hour AM_PM Concatenated_Time Violation_Time_bins
# 1             20               0407P    4     P               4:P                Dusk
# 2             70               1119A   11     A              11:A                 Day
# 3             17               0751A    7     A               7:A                Dawn
# 4             74               1105A   11     A              11:A                 Day
# 5             21               0423A    4     A               4:A                Dawn
# 6             20               0616A    6     A               6:A                Dawn




### Finding 3 most commonly occurring violations for each of these groups

### Year: 2015

# A) 0 AM - 3 AM = Midnight

top_3_Violation_Time_midnight_2015 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                                  count(*) as no_of_tickets
                                                  from Violation_Time_bins_2015_sql
                                                  where Violation_Time_bins = 'MidNight' 
                                                  group by Violation_Code
                                                  order by count(*) desc
                                                  limit 3")


top_3_Violation_Time_midnight_2015<-head(top_3_Violation_Time_midnight_2015)
top_3_Violation_Time_midnight_2015$year<-c(replicate(3,'2015'))
top_3_Violation_Time_midnight_2015$time_group<-c(replicate(3,'MidNight'))
top_3_Violation_Time_midnight_2015

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             21         65761 2015   MidNight
# 2             40         40711 2015   MidNight
# 3             78         37901 2015   MidNight



# B) 4 AM - 7 AM = Dawn

top_3_Violation_Time_dawn_2015 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                              count(*) as no_of_tickets 
                                              from Violation_Time_bins_2015_sql
                                              where Violation_Time_bins = 'Dawn' 
                                              group by Violation_Code
                                              order by count(*) desc    
                                              limit 3")


top_3_Violation_Time_dawn_2015<-head(top_3_Violation_Time_dawn_2015)
top_3_Violation_Time_dawn_2015$year<-c(replicate(3,'2015'))
top_3_Violation_Time_dawn_2015$time_group<-c(replicate(3,'Dawn'))
top_3_Violation_Time_dawn_2015

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             14        132344 2015       Dawn
# 2             21        103874 2015       Dawn
# 3             40         89611 2015       Dawn


# C) 8 AM - 11AM = Day

top_3_Violation_Time_day_2015 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                             count(*) as no_of_tickets 
                                             from Violation_Time_bins_2015_sql
                                             where Violation_Time_bins = 'Day' 
                                             group by Violation_Code
                                             order by count(*) desc
                                             limit 3")


top_3_Violation_Time_day_2015<-head(top_3_Violation_Time_day_2015)
top_3_Violation_Time_day_2015$year<-c(replicate(3,'2015'))
top_3_Violation_Time_day_2015$time_group<-c(replicate(3,'Day'))
top_3_Violation_Time_day_2015

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             21       1167097 2015        Day
# 2             38        442655 2015        Day
# 3             36        353555 2015        Day


# D) 12PM - 3 PM = Noon

top_3_Violation_Time_noon_2015 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                              count(*) as no_of_tickets
                                              from Violation_Time_bins_2015_sql
                                              where Violation_Time_bins = 'Noon' 
                                              group by Violation_Code
                                              order by count(*) desc
                                              limit 3")


top_3_Violation_Time_noon_2015<-head(top_3_Violation_Time_noon_2015)
top_3_Violation_Time_noon_2015$year<-c(replicate(3,'2015'))
top_3_Violation_Time_noon_2015$time_group<-c(replicate(3,'Noon'))
top_3_Violation_Time_noon_2015

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             38        559953 2015       Noon
# 2             37        411845 2015       Noon
# 3             36        317228 2015       Noon


# E) 4 PM - 7 PM = Dusk

top_3_Violation_Time_dusk_2015 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                              count(*) as no_of_tickets
                                              from Violation_Time_bins_2015_sql
                                              where Violation_Time_bins = 'Dusk' 
                                              group by Violation_Code
                                              order by count(*) desc
                                              limit 3")


top_3_Violation_Time_dusk_2015<-head(top_3_Violation_Time_dusk_2015)
top_3_Violation_Time_dusk_2015$year<-c(replicate(3,'2015'))
top_3_Violation_Time_dusk_2015$time_group<-c(replicate(3,'Dusk'))
top_3_Violation_Time_dusk_2015


# Results:

#   Violation_Code no_of_tickets year time_group
# 1             38        237513 2015       Dusk
# 2             37        173008 2015       Dusk
# 3             14        145602 2015       Dusk


# F) 8 PM - 11 PM = Night

top_3_Violation_Time_night_2015 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2015_sql
                                               where Violation_Time_bins = 'Night' 
                                               group by Violation_Code
                                               order by count(*) desc
                                               limit 3")


top_3_Violation_Time_night_2015<-head(top_3_Violation_Time_night_2015)
top_3_Violation_Time_night_2015$year<-c(replicate(3,'2015'))
top_3_Violation_Time_night_2015$time_group<-c(replicate(3,'Night'))
top_3_Violation_Time_night_2015


# Results:

#   Violation_Code no_of_tickets year time_group
# 1              7         69973 2015      Night
# 2             38         61530 2015      Night
# 3             14         44554 2015      Night


###Combining the data of all time bins together for the year 2015
top_3_Violation_Time_2015<-rbind(top_3_Violation_Time_midnight_2015,top_3_Violation_Time_dawn_2015,top_3_Violation_Time_day_2015,top_3_Violation_Time_noon_2015,top_3_Violation_Time_dusk_2015,top_3_Violation_Time_night_2015)
top_3_Violation_Time_2015

###Plotting graph for finding the top 3 violation time per time bins
plot_top_3_Violation_Time_2015<-ggplot(top_3_Violation_Time_2015,aes(x=time_group,y=no_of_tickets)) + 
  geom_bar(aes(fill=factor(Violation_Code)),stat="identity")+theme_bw() +xlab('Time bins') + ylab('Count of tickets') + 
  ggtitle('Year 2015 : Top 3 violation codes per time bins')

plot_top_3_Violation_Time_2015

### Year: 2016

# A) 0 AM - 3 AM = Midnight

top_3_Violation_Time_midnight_2016 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                                  count(*) as no_of_tickets
                                                  from Violation_Time_bins_2016_sql
                                                  where Violation_Time_bins = 'MidNight' 
                                                  group by Violation_Code
                                                  order by count(*) desc
                                                  limit 3")


top_3_Violation_Time_midnight_2016<-head(top_3_Violation_Time_midnight_2016)
top_3_Violation_Time_midnight_2016$year<-c(replicate(3,'2016'))
top_3_Violation_Time_midnight_2016$time_group<-c(replicate(3,'MidNight'))
top_3_Violation_Time_midnight_2016


# Results:

# Violation_Code no_of_tickets year time_group
# 1             21         70452 2016   MidNight
# 2             40         40668 2016   MidNight
# 3             78         31641 2016   MidNight


# B) 4 AM - 7 AM = Dawn

top_3_Violation_Time_dawn_2016 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                              count(*) as no_of_tickets
                                              from Violation_Time_bins_2016_sql
                                              where Violation_Time_bins = 'Dawn' 
                                              group by Violation_Code
                                              order by count(*) desc
                                              limit 3")


top_3_Violation_Time_dawn_2016<-head(top_3_Violation_Time_dawn_2016)
top_3_Violation_Time_dawn_2016$year<-c(replicate(3,'2016'))
top_3_Violation_Time_dawn_2016$time_group<-c(replicate(3,'Dawn'))
top_3_Violation_Time_dawn_2016

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             14        137946 2016       Dawn
# 2             21        110889 2016       Dawn
# 3             40         89708 2016       Dawn


# C) 8 AM - 11AM = Day

top_3_Violation_Time_day_2016 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                             count(*) as no_of_tickets
                                             from Violation_Time_bins_2016_sql
                                             where Violation_Time_bins = 'Day' 
                                             group by Violation_Code
                                             order by count(*) desc
                                             limit 3")


top_3_Violation_Time_day_2016<-head(top_3_Violation_Time_day_2016)
top_3_Violation_Time_day_2016$year<-c(replicate(3,'2016'))
top_3_Violation_Time_day_2016$time_group<-c(replicate(3,'Day'))
top_3_Violation_Time_day_2016

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             21       1183377 2016        Day
# 2             36        578035 2016        Day
# 3             38        382100 2016        Day



# D) 12PM - 3 PM = Noon

top_3_Violation_Time_noon_2016 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                              count(*) as no_of_tickets
                                              from Violation_Time_bins_2016_sql
                                              where Violation_Time_bins = 'Noon' 
                                              group by Violation_Code
                                              order by count(*) desc
                                              limit 3")


top_3_Violation_Time_noon_2016<-head(top_3_Violation_Time_noon_2016)
top_3_Violation_Time_noon_2016$year<-c(replicate(3,'2016'))
top_3_Violation_Time_noon_2016$time_group<-c(replicate(3,'Noon'))
top_3_Violation_Time_noon_2016

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             36        536551 2016       Noon
# 2             38        480842 2016       Noon
# 3             37        378365 2016       Noon


# E) 4 PM - 7 PM = Dusk

top_3_Violation_Time_dusk_2016 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                              count(*) as no_of_tickets
                                              from Violation_Time_bins_2016_sql
                                              where Violation_Time_bins = 'Dusk' 
                                              group by Violation_Code
                                              order by count(*) desc
                                              limit 3")


top_3_Violation_Time_dusk_2016<-head(top_3_Violation_Time_dusk_2016)
top_3_Violation_Time_dusk_2016$year<-c(replicate(3,'2016'))
top_3_Violation_Time_dusk_2016$time_group<-c(replicate(3,'Dusk'))
top_3_Violation_Time_dusk_2016

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             38        208759 2016       Dusk
# 2             37        159810 2016       Dusk
# 3             14        132446 2016       Dusk



# F) 8 PM - 11 PM = Night

top_3_Violation_Time_night_2016 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2016_sql
                                               where Violation_Time_bins = 'Night' 
                                               group by Violation_Code
                                               order by count(*) desc
                                               limit 3")


top_3_Violation_Time_night_2016<-head(top_3_Violation_Time_night_2016)
top_3_Violation_Time_night_2016$year<-c(replicate(3,'2016'))
top_3_Violation_Time_night_2016$time_group<-c(replicate(3,'Night'))
top_3_Violation_Time_night_2016


# Results:

#   Violation_Code no_of_tickets year time_group
# 1              7         56836 2016      Night
# 2             38         52582 2016      Night
# 3             40         43936 2016      Night


###Combining the data of all time bins together for the year 2016
top_3_Violation_Time_2016<-rbind(top_3_Violation_Time_midnight_2016,top_3_Violation_Time_dawn_2016,top_3_Violation_Time_day_2016,top_3_Violation_Time_noon_2016,top_3_Violation_Time_dusk_2016,top_3_Violation_Time_night_2016)
top_3_Violation_Time_2016

###Plotting graph for finding the top 3 violation time per time bins
plot_top_3_Violation_Time_2016<-ggplot(top_3_Violation_Time_2016,aes(x=time_group,y=no_of_tickets )) +
                                geom_bar(aes(fill=factor(Violation_Code)),stat="identity") + theme_bw()+ xlab('Time bins') + ylab('Count of tickets') + 
                                ggtitle('Year 2016 : Top 3 violation codes per time bins')

plot_top_3_Violation_Time_2016


### Year: 2017

# A) 0 AM - 3 AM = Midnight

top_3_Violation_Time_midnight_2017 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                                  count(*) as no_of_tickets
                                                  from Violation_Time_bins_2017_sql
                                                  where Violation_Time_bins = 'MidNight' 
                                                  group by Violation_Code
                                                  order by count(*) desc
                                                  limit 3")


top_3_Violation_Time_midnight_2017<-head(top_3_Violation_Time_midnight_2017)
top_3_Violation_Time_midnight_2017$year<-c(replicate(3,'2017'))
top_3_Violation_Time_midnight_2017$time_group<-c(replicate(3,'Midnight'))
top_3_Violation_Time_midnight_2017

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             21         75800 2017   Midnight
# 2             40         49467 2017   Midnight
# 3             14         31334 2017   Midnight



# B) 4 AM - 7 AM = Dawn

top_3_Violation_Time_dawn_2017 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                              count(*) as no_of_tickets
                                              from Violation_Time_bins_2017_sql
                                              where Violation_Time_bins = 'Dawn' 
                                              group by Violation_Code
                                              order by count(*) desc
                                              limit 3")


top_3_Violation_Time_dawn_2017<-head(top_3_Violation_Time_dawn_2017)
top_3_Violation_Time_dawn_2017$year<-c(replicate(3,'2017'))
top_3_Violation_Time_dawn_2017$time_group<-c(replicate(3,'Dawn'))
top_3_Violation_Time_dawn_2017


# Results:


#   Violation_Code no_of_tickets year time_group
# 1             14        139373 2017       Dawn
# 2             21        116862 2017       Dawn
# 3             40        110630 2017       Dawn



# C) 8 AM - 11AM = Day

top_3_Violation_Time_day_2017 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                             count(*) as no_of_tickets
                                             from Violation_Time_bins_2017_sql
                                             where Violation_Time_bins = 'Day' 
                                             group by Violation_Code
                                             order by count(*) desc
                                             limit 3")


top_3_Violation_Time_day_2017<-head(top_3_Violation_Time_day_2017)
top_3_Violation_Time_day_2017$year<-c(replicate(3,'2017'))
top_3_Violation_Time_day_2017$time_group<-c(replicate(3,'Day'))
top_3_Violation_Time_day_2017

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             21       1161233 2017        Day
# 2             36        726513 2017        Day
# 3             38        343204 2017        Day



# D) 12PM - 3 PM = Noon

top_3_Violation_Time_noon_2017 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                              count(*) as no_of_tickets
                                              from Violation_Time_bins_2017_sql
                                              where Violation_Time_bins = 'Noon' 
                                              group by Violation_Code
                                              order by count(*) desc
                                              limit 3")


top_3_Violation_Time_noon_2017<-head(top_3_Violation_Time_noon_2017)
top_3_Violation_Time_noon_2017$year<-c(replicate(3,'2017'))
top_3_Violation_Time_noon_2017$time_group<-c(replicate(3,'Noon'))
top_3_Violation_Time_noon_2017

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             36        563564 2017       Noon
# 2             38        457190 2017       Noon
# 3             37        332458 2017       Noon



# E) 4 PM - 7 PM = Dusk

top_3_Violation_Time_dusk_2017 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                              count(*) as no_of_tickets
                                              from Violation_Time_bins_2017_sql
                                              where Violation_Time_bins = 'Dusk' 
                                              group by Violation_Code
                                              order by count(*) desc
                                              limit 3")


top_3_Violation_Time_dusk_2017<-head(top_3_Violation_Time_dusk_2017)
top_3_Violation_Time_dusk_2017$year<-c(replicate(3,'2017'))
top_3_Violation_Time_dusk_2017$time_group<-c(replicate(3,'Dusk'))
top_3_Violation_Time_dusk_2017

# Results:

#   Violation_Code no_of_tickets year time_group
# 1             38        200785 2017       Dusk
# 2             37        143593 2017       Dusk
# 3             14        142317 2017       Dusk



# F) 8 PM - 11 PM = Night

top_3_Violation_Time_night_2017 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2017_sql
                                               where Violation_Time_bins = 'Night' 
                                               group by Violation_Code
                                               order by count(*) desc
                                               limit 3")


top_3_Violation_Time_night_2017<-head(top_3_Violation_Time_night_2017)
top_3_Violation_Time_night_2017$year<-c(replicate(3,'2017'))
top_3_Violation_Time_night_2017$time_group<-c(replicate(3,'Night'))
top_3_Violation_Time_night_2017


# Results:

# Violation_Code no_of_tickets year time_group
# 1              7         59134 2017      Night
# 2             38         46499 2017      Night
# 3             14         43889 2017      Night

###Combining the data of all time bins together for the year 2017
top_3_Violation_Time_2017<-rbind(top_3_Violation_Time_midnight_2017,top_3_Violation_Time_dawn_2017,top_3_Violation_Time_day_2017,top_3_Violation_Time_noon_2017,top_3_Violation_Time_dusk_2017,top_3_Violation_Time_night_2017)
top_3_Violation_Time_2017

###Plotting graph for finding the top 3 violation time per time bins
plot_top_3_Violation_Time_2017<-ggplot(top_3_Violation_Time_2017,aes(x=time_group,y=no_of_tickets )) + 
                                geom_bar(aes(fill= factor(Violation_Code)),stat="identity")+theme_bw()+xlab('Time bins') + ylab('Count of tickets') + 
                                ggtitle('Year 2017 : Top 3 violation codes per time bins')
plot_top_3_Violation_Time_2017

###Combining the data of all time bins together for all years : 2015,2016 and 2017
top_3_Violation_Time<-rbind(top_3_Violation_Time_2017,top_3_Violation_Time_2016,top_3_Violation_Time_2015)
top_3_Violation_Time

###Plotting graph for finding the top 3 violation time per time bins for all years
plot_top_3_Violation_Time<- ggplot(top_3_Violation_Time, aes(factor(Violation_Code), no_of_tickets)) + geom_col() + 
                            facet_grid(year~time_group)+ xlab('Violation Code') + ylab('Tickets Issued') +
                            ggtitle('Top 3 violation codes per time bins for all years') + theme(axis.text.x = element_text(angle = 90, vjust = 0.5))

plot_top_3_Violation_Time


######    D) Now, try another direction. 
######       For the 3 most commonly occurring violation codes, 
######       Find the most common times of day (in terms of the bins from the previous part)


### Year: 2015 

# Top 3 "Violation Codes" for year 2015
violation_code_top3_2015 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                        count(*) as no_of_tickets 
                                        from Violation_Time_bins_2015_sql 
                                        group by Violation_Code
                                        order by count(*) desc
                                        limit 3")

violation_code_top3_df_2015 <- collect(violation_code_top3_2015)

violation_code_top3_df_2015

# Results:

#     Violation_Code  no_of_tickets
# 1                  21     1469197
# 2                  38     1305007
# 3                  14      908402


# 3 most common times of day for most occuring violation code = 21, of year 2015

common_times_top3_viocode1_2015 <- SparkR::sql("select Violation_Time_bins, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2015_sql
                                               where Violation_Code = 21
                                               group by Violation_Time_bins
                                               order by count(*) desc
                                               limit 3")


common_times_top3_viocode1_df_2015 <- collect(common_times_top3_viocode1_2015)
common_times_top3_viocode1_df_2015$year <- c(replicate(3,'2015'))
common_times_top3_viocode1_df_2015$Violation_Code<-c(replicate(3,'21'))
common_times_top3_viocode1_df_2015

# Results:

#   Violation_Time_bins no_of_tickets year Violation_Code
# 1                 Day       1167097 2015             21
# 2                Noon        131160 2015             21
# 3                Dawn        103874 2015             21


# 3 most common times of day for second most occuring violation code = 38, of year 2015

common_times_top3_viocode2_2015 <- SparkR::sql("select Violation_Time_bins, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2015_sql
                                               where Violation_Code = 38
                                               group by Violation_Time_bins
                                               order by count(*) desc
                                               limit 3")


common_times_top3_viocode2_df_2015 <- collect(common_times_top3_viocode2_2015)
common_times_top3_viocode2_df_2015$year <- c(replicate(3,'2015'))
common_times_top3_viocode2_df_2015$Violation_Code <- c(replicate(3,'38'))
common_times_top3_viocode2_df_2015

# Results:

#   Violation_Time_bins no_of_tickets year Violation_Code
# 1                Noon        559953 2015             38
# 2                 Day        442655 2015             38
# 3                Dusk        237513 2015             38



# 3 most common times of day for third most occuring violation code = 14, of year 2015

common_times_top3_viocode3_2015 <- SparkR::sql("select Violation_Time_bins, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2015_sql
                                               where Violation_Code = 14
                                               group by Violation_Time_bins
                                               order by count(*) desc
                                               limit 3")


common_times_top3_viocode3_df_2015 <- collect(common_times_top3_viocode3_2015)
common_times_top3_viocode3_df_2015$year <- c(replicate(3,'2015'))
common_times_top3_viocode3_df_2015$Violation_Code <- c(replicate(3,'14'))
common_times_top3_viocode3_df_2015


# Results:

#   Violation_Time_bins no_of_tickets year Violation_Code
# 1                 Day        292903 2015             14
# 2                Noon        263615 2015             14
# 3                Dusk        145602 2015             14


###Combining the data of all the top 3 violation codes
common_times_top3_viocode_df_2015<- rbind(common_times_top3_viocode1_df_2015,common_times_top3_viocode2_df_2015,common_times_top3_viocode3_df_2015)
common_times_top3_viocode_df_2015

###Plotting graph for comparing data of top 3 violation codes as per times of day
plot_common_times_top3_viocode_df_2015<-ggplot(common_times_top3_viocode_df_2015, aes(x=Violation_Code ,y=no_of_tickets)) + 
                                        geom_bar(aes(fill= Violation_Time_bins),stat="identity", width = 0.5)+theme_bw()+xlab('Violation Codes') + ylab('Count of tickets') + 
                                        ggtitle('Year 2015: Top 3 Common Times of Day for most occuring Violation Codes')

plot_common_times_top3_viocode_df_2015


### Year: 2016

# Top 3 "Violation Codes" for year 2016
violation_code_top3_2016 <- SparkR::sql("select Violation_Code as Violation_Code_2016, 
                                        count(*) as no_of_tickets 
                                        from Violation_Time_bins_2016_sql 
                                        group by Violation_Code
                                        order by count(*) desc
                                        limit 3")

violation_code_top3_df_2016 <- collect(violation_code_top3_2016)

violation_code_top3_df_2016

# Results:

#      Violation_Code_2016   no_of_tickets
# 1                  21       1497223
# 2                  36       1232951
# 3                  38       1126834

# 3 most common times of day for most occuring violation code = 21, of year 2016

common_times_top3_viocode1_2016 <- SparkR::sql("select Violation_Time_bins, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2016_sql
                                               where Violation_Code = 21
                                               group by Violation_Time_bins
                                               order by count(*) desc
                                               limit 3")


common_times_top3_viocode1_df_2016 <- collect(common_times_top3_viocode1_2016)
common_times_top3_viocode1_df_2016$year <- c(replicate(3,'2016'))
common_times_top3_viocode1_df_2016$Violation_Code <- c(replicate(3,'21'))
common_times_top3_viocode1_df_2016


# Results:

#   Violation_Time_bins no_of_tickets year Violation_Code
# 1                 Day       1183377 2016             21
# 2                Noon        131539 2016             21
# 3                Dawn        110889 2016             21


# 3 most common times of day for second most occuring violation code = 36, of year 2016

common_times_top3_viocode2_2016 <- SparkR::sql("select Violation_Time_bins, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2016_sql
                                               where Violation_Code = 36
                                               group by Violation_Time_bins
                                               order by count(*) desc
                                               limit 3")


common_times_top3_viocode2_df_2016 <- collect(common_times_top3_viocode2_2016)
common_times_top3_viocode2_df_2016$year <- c(replicate(3,'2016'))
common_times_top3_viocode2_df_2016$Violation_Code <- c(replicate(3,'36'))
common_times_top3_viocode2_df_2016

# Results:

#   Violation_Time_bins no_of_tickets year Violation_Code
# 1                 Day        578035 2016             36
# 2                Noon        536551 2016             36
# 3                Dawn         77657 2016             36



# 3 most common times of day for third most occuring violation code = 38, of year 2016

common_times_top3_viocode3_2016 <- SparkR::sql("select Violation_Time_bins, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2016_sql
                                               where Violation_Code = 38
                                               group by Violation_Time_bins
                                               order by count(*) desc
                                               limit 3")


common_times_top3_viocode3_df_2016 <- collect(common_times_top3_viocode3_2016)
common_times_top3_viocode3_df_2016$year <- c(replicate(3,'2016'))
common_times_top3_viocode3_df_2016$Violation_Code <- c(replicate(3,'38'))
common_times_top3_viocode3_df_2016


# Results:

#   Violation_Time_bins no_of_tickets year Violation_Code
# 1                Noon        480842 2016             38
# 2                 Day        382100 2016             38
# 3                Dusk        208759 2016             38



###Combining the data of all the top 3 violation codes
common_times_top3_viocode_df_2016<- rbind(common_times_top3_viocode1_df_2016,common_times_top3_viocode2_df_2016,common_times_top3_viocode3_df_2016)
common_times_top3_viocode_df_2016

###Plotting graph for comparing data of top 3 violation codes as per times of day
plot_common_times_top3_viocode_df_2016 <- ggplot(common_times_top3_viocode_df_2016, aes(x=Violation_Code ,y=no_of_tickets)) + 
                                          geom_bar(aes(fill= Violation_Time_bins),stat="identity", width = 0.5)+theme_bw()+xlab('Violation Codes') + ylab('Count of tickets') + 
                                          ggtitle('Year 2016: Top 3 Common Times of Day for most occuring Violation Codes')

plot_common_times_top3_viocode_df_2016


### Year: 2017

# Top 3 "Violation Codes" for year 2017
violation_code_top3_2017 <- SparkR::sql("select Violation_Code as Violation_Code_2017, 
                                        count(*) as no_of_tickets 
                                        from Violation_Time_bins_2017_sql 
                                        group by Violation_Code
                                        order by count(*) desc
                                        limit 3")

violation_code_top3_df_2017 <- collect(violation_code_top3_2017)

violation_code_top3_df_2017

# Results:

#      Violation_Code_2017  no_of_tickets
# 1                  21       1500358
# 2                  36       1345237
# 3                  38       1050415


# 3 most common times of day for most occuring violation code = 21, of year 2017

common_times_top3_viocode1_2017 <- SparkR::sql("select Violation_Time_bins, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2017_sql
                                               where Violation_Code = 21
                                               group by Violation_Time_bins
                                               order by count(*) desc
                                               limit 3")


common_times_top3_viocode1_df_2017 <- collect(common_times_top3_viocode1_2017)
common_times_top3_viocode1_df_2017$year <- c(replicate(3,'2017'))
common_times_top3_viocode1_df_2017$Violation_Code <- c(replicate(3,'21'))
common_times_top3_viocode1_df_2017

# Results:

#   Violation_Time_bins no_of_tickets year Violation_Code
# 1                 Day       1161233 2017             21
# 2                Noon        145604 2017             21
# 3                Dawn        116862 2017             21


# 3 most common times of day for second most occuring violation code = 36, of year 2017

common_times_top3_viocode2_2017 <- SparkR::sql("select Violation_Time_bins, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2017_sql
                                               where Violation_Code = 36
                                               group by Violation_Time_bins
                                               order by count(*) desc
                                               limit 3")


common_times_top3_viocode2_df_2017 <- collect(common_times_top3_viocode2_2017)
common_times_top3_viocode2_df_2017$year <- c(replicate(3,'2017'))
common_times_top3_viocode2_df_2017$Violation_Code <- c(replicate(3,'36'))
common_times_top3_viocode2_df_2017


# Results:

#   Violation_Time_bins no_of_tickets year Violation_Code
# 1                 Day        726513 2017             36
# 2                Noon        563564 2017             36
# 3                Dawn         30072 2017             36


# 3 most common times of day for third most occuring violation code = 38, of year 2017

common_times_top3_viocode3_2017 <- SparkR::sql("select Violation_Time_bins, 
                                               count(*) as no_of_tickets
                                               from Violation_Time_bins_2017_sql
                                               where Violation_Code = 38
                                               group by Violation_Time_bins
                                               order by count(*) desc
                                               limit 3")


common_times_top3_viocode3_df_2017 <- collect(common_times_top3_viocode3_2017)
common_times_top3_viocode3_df_2017$year <- c(replicate(3,'2017'))
common_times_top3_viocode3_df_2017$Violation_Code <- c(replicate(3,'38'))
common_times_top3_viocode3_df_2017

# Results:

#   Violation_Time_bins no_of_tickets year Violation_Code
# 1                Noon        457190 2017             38
# 2                 Day        343204 2017             38
# 3                Dusk        200785 2017             38


###Combining the data of all the top 3 violation codes
common_times_top3_viocode_df_2017<- rbind(common_times_top3_viocode1_df_2017,common_times_top3_viocode2_df_2017,common_times_top3_viocode3_df_2017)
common_times_top3_viocode_df_2017

###Plotting graph for comparing data of top 3 violation codes as per times of day
plot_common_times_top3_viocode_df_2017 <- ggplot(common_times_top3_viocode_df_2017, aes(x=Violation_Code ,y=no_of_tickets)) + 
                                          geom_bar(aes(fill= Violation_Time_bins),stat="identity", width = 0.5)+theme_bw()+xlab('Violation Codes') + ylab('Count of tickets') + 
                                          ggtitle('Year 2017: Top 3 Common Times of Day for most occuring Violation Codes')

plot_common_times_top3_viocode_df_2017


###Combining the data of all most occurring violation codes together for all years : 2015,2016 and 2017
common_times_top3_viocode_df_all <-rbind(common_times_top3_viocode_df_2015, common_times_top3_viocode_df_2016, common_times_top3_viocode_df_2017)
common_times_top3_viocode_df_all

###Plotting graph for finding the top 3 common times for most occuring violation codes for all years
plot_common_times_top3_viocode <- ggplot(common_times_top3_viocode_df_all, aes(Violation_Time_bins, no_of_tickets)) + geom_col() + 
                                  facet_grid(year~Violation_Code)+ xlab('Violation_Time_bins') + ylab('Tickets Issued') +
                                  ggtitle('Top 3 Common Times of Day for most occurring VioCodes for all years') + theme(axis.text.x = element_text(angle = 90, vjust = 0.5))

plot_common_times_top3_viocode


############################################################################################################################


###### 6) Lets try and find some seasonality in this data
######    First, divide the year into some number of seasons, 
######    and find frequencies of tickets for each season.
######    Then, find the 3 most common violations for each of these season

# According to the meteorological definition, the seasons begin on the first day of the months 
# that include the equinoxes and solstices:

# Spring runs from March 1 to May 31;
# Summer runs from June 1 to August 31;
# Fall (autumn) runs from September 1 to November 30; and
# Winter runs from December 1 to February 28 (February 29 in a leap year).


### Year: 2015
month_data_2015 <- SparkR::sql("select `Violation Code` as Violation_Code,
                               TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)) as Date,  
                               MONTH(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as Month 
                               from parking_2015_sql")

createOrReplaceTempView(month_data_2015, "month_data_2015_sql")

distinct_month_data_2015 <- SparkR::sql("select distinct MONTH(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as distinct_month 
                                        from parking_2015_sql")


head(month_data_2015, 5)

# Results:

#     Violation_Code  Date       Month
# 1              20 2014-09-08     9
# 2              70 2015-04-15     4
# 3              20 2014-09-30     9
# 4              74 2015-06-14     6
# 5              20 2014-11-23    11


distinct_month_data_df_2015 <- collect(distinct_month_data_2015)
distinct_month_data_df_2015

# Results:

#       distinct_month
# 1              12
# 2               1
# 3               6
# 4               3
# 5               5
# 6               9
# 7               4
# 8               8
# 9               7
# 10             10
# 11             11
# 12              2

# The distinct month dataframe was just to check for invalid months, if any. But we found no errors and
# are good to go for binning the months column for seasons.




# Binning months into different seasons
bins_months_data_2015 <- SparkR::sql("select Violation_Code, Date, Month, \
                                     case when (month >= 3 and month <= 5) then 'Spring'\
                                     when (month >= 6 and month <= 8) then 'Summer'\
                                     when (month >= 9 and month <= 11) then 'Fall'\
                                     else 'Winter' end as bin_months from month_data_2015_sql")


createOrReplaceTempView(bins_months_data_2015, "bins_months_data_2015_sql")

head(bins_months_data_2015)

# Results:

#  Violation_Code     Date     Month bin_months
# 1             20 2014-09-08     9       Fall
# 2             70 2015-04-15     4     Spring
# 3             20 2014-09-30     9       Fall
# 4             74 2015-06-14     6     Summer
# 5             20 2014-11-23    11       Fall
# 6             14 2014-10-19    10       Fall



### Frequencies of Tickets For Each Season is:
seasonal_frequency_2015 <- SparkR::sql("select bin_months as Seasons, 
                                       count(*) as Frequencies_of_Tickets 
                                       from bins_months_data_2015_sql 
                                       group by bin_months
                                       order by count(*) desc")

seasonal_frequency_df_2015 <- collect(seasonal_frequency_2015)
seasonal_frequency_df_2015$year<-c(replicate(4,'2015'))
seasonal_frequency_df_2015
# Results:

#   Seasons Frequencies_of_Tickets year
# 1  Spring                2860987 2015
# 2  Summer                2838306 2015
# 3    Fall                2718502 2015
# 4  Winter                2180240 2015

# Plotting Seasonal Frequency for year 2015
plot_seasonalfreq_2015 <- ggplot(seasonal_frequency_df_2015,aes(Seasons,Frequencies_of_Tickets))+ geom_col(width = 0.5) + 
                          geom_text(aes(label = Frequencies_of_Tickets), vjust = -0.25) + xlab('Seasons') + ylab('Count of tickets') + 
                          ggtitle('Year 2015 : Frequency of tickets per season') + theme_grey()

plot_seasonalfreq_2015

### 3 most violations for each season

# Season: Spring
most_violation_spring_2015 <- SparkR::sql("select Violation_Code, 
                                          count(*) as Frequencies_of_Tickets 
                                          from bins_months_data_2015_sql  
                                          where bin_months = 'Spring'
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")

most_violation_spring_df_2015 <- collect(most_violation_spring_2015)
most_violation_spring_df_2015$year <- c(replicate(3,'2015'))
most_violation_spring_df_2015$Season <- c(replicate(3,'Spring'))
most_violation_spring_df_2015

# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             21                 425163 2015 Spring
# 2             38                 327048 2015 Spring
# 3             14                 243622 2015 Spring

# Season: Summer
most_violation_summer_2015 <- SparkR::sql("select Violation_Code, 
                                          count(*) as Frequencies_of_Tickets 
                                          from bins_months_data_2015_sql  
                                          where bin_months = 'Summer'
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")

most_violation_summer_df_2015 <- collect(most_violation_summer_2015)
most_violation_summer_df_2015$year <- c(replicate(3,'2015'))
most_violation_summer_df_2015$Season <- c(replicate(3,'Summer'))
most_violation_summer_df_2015

# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             21                 439632 2015 Summer
# 2             38                 344262 2015 Summer
# 3             14                 239339 2015 Summer



# Season: Fall
most_violation_fall_2015 <- SparkR::sql("select Violation_Code, 
                                        count(*) as Frequencies_of_Tickets 
                                        from bins_months_data_2015_sql  
                                        where bin_months = 'Fall'
                                        group by Violation_Code
                                        order by count(*) desc
                                        limit 3")

most_violation_fall_df_2015 <- collect(most_violation_fall_2015)
most_violation_fall_df_2015$year <- c(replicate(3,'2015'))
most_violation_fall_df_2015$Season <- c(replicate(3,'Fall'))
most_violation_fall_df_2015

# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             21                 351390 2015   Fall
# 2             38                 326700 2015   Fall
# 3             14                 232300 2015   Fall


# Season: Winter
most_violation_winter_2015 <- SparkR::sql("select Violation_Code, 
                                          count(*) as Frequencies_of_Tickets 
                                          from bins_months_data_2015_sql  
                                          where bin_months = 'Winter'
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")

most_violation_winter_df_2015 <- collect(most_violation_winter_2015)
most_violation_winter_df_2015$year <- c(replicate(3,'2015'))
most_violation_winter_df_2015$Season <- c(replicate(3,'Winter'))
most_violation_winter_df_2015

# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             38                 306997 2015 Winter
# 2             21                 253043 2015 Winter
# 3             14                 193157 2015 Winter


### Combining the violation code results for 2015
most_violation_df_2015 <- rbind(most_violation_spring_df_2015,most_violation_summer_df_2015,most_violation_fall_df_2015,most_violation_winter_df_2015)
most_violation_df_2015

# Plotting the comparison of count(violation code per seasons)

plot_most_violation_df_2015 <- ggplot(most_violation_df_2015,aes(Violation_Code ,Frequencies_of_Tickets ))+geom_bar((Season),stat = "identity")+xlab('Seasons 2015')+ylab('count of tickets') + 
                               ggtitle("Frequency of violation code per seasons for 2015")

plot_most_violation_df_2015

###Plotting graph for finding the top 3 Violation Codes for each Season of year 2015
plot_top_3_Violation_Code_Season_2015 <- ggplot(most_violation_df_2015,aes(x=Season,y=Frequencies_of_Tickets )) + 
                                         geom_bar(aes(fill=factor(Violation_Code)),stat="identity")+theme_bw() +xlab('Season') + ylab('Count of tickets') + 
                                         ggtitle('Year 2015 : Top 3 violation codes for each Season')

plot_top_3_Violation_Code_Season_2015



### Year: 2016
month_data_2016 <- SparkR::sql("select `Violation Code` as Violation_Code,
                               TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)) as Date,  
                               MONTH(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as Month 
                               from parking_2016_sql")

createOrReplaceTempView(month_data_2016, "month_data_2016_sql")

distinct_month_data_2016 <- SparkR::sql("select distinct MONTH(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as distinct_month
                                        from parking_2016_sql")


head(month_data_2016, 5)

# Results:

#     Violation_Code    Date    Month
# 1              24 2015-07-30     7
# 2              20 2015-09-05     9
# 3              14 2015-11-07    11
# 4              17 2015-12-15    12
# 5              68 2015-07-22     7

distinct_month_data_df_2016 <- collect(distinct_month_data_2016)
distinct_month_data_df_2016

# Results:

#        distinct_month
# 1              12
# 2               1
# 3               6
# 4               3
# 5               5
# 6               9
# 7               4
# 8               8
# 9               7
# 10             10
# 11             11
# 12              2



# The distinct month dataframe was just to check for invalid months, if any. But we found no errors and
# are good to go for binning the months column for seasons.


# Binning months into different seasons
bins_months_data_2016 <- SparkR::sql("select Violation_Code, Date, Month, \
                                     case when (month >= 3 and month <= 5) then 'Spring'\
                                     when (month >= 6 and month <= 8) then 'Summer'\
                                     when (month >= 9 and month <= 11) then 'Fall'\
                                     else 'Winter' end as bin_months from month_data_2016_sql")

createOrReplaceTempView(bins_months_data_2016, "bins_months_data_2016_sql")


head(bins_months_data_2016)

# Results:

#   Violation_Code    Date     Month bin_months
# 1             24 2015-07-30     7     Summer
# 2             20 2015-09-05     9       Fall
# 3             14 2015-11-07    11       Fall
# 4             17 2015-12-15    12     Winter
# 5             68 2015-07-22     7     Summer
# 6             46 2015-07-14     7     Summer



### Frequencies of Tickets For Each Season is:
seasonal_frequency_2016 <- SparkR::sql("select bin_months as Seasons, 
                                       count(*) as Frequencies_of_Tickets 
                                       from bins_months_data_2016_sql 
                                       group by bin_months
                                       order by count(*) desc")

seasonal_frequency_df_2016 <- collect(seasonal_frequency_2016)
seasonal_frequency_df_2016$year<-c(replicate(4,'2016'))
seasonal_frequency_df_2016

# Results:

#   Seasons Frequencies_of_Tickets year
# 1    Fall                2971672 2016
# 2  Spring                2789066 2016
# 3  Winter                2421620 2016
# 4  Summer                2214536 2016


# Plotting Seasonal Frequency for year 2015

plot_seasonalfreq_2016 <- ggplot(seasonal_frequency_df_2016,aes(Seasons,Frequencies_of_Tickets))+ geom_col(width = 0.5) + 
                          geom_text(aes(label = Frequencies_of_Tickets), vjust = -0.25) + xlab('Seasons') + ylab('Count of tickets') + 
                          ggtitle('Year 2016 : Frequency of tickets per season') + theme_grey()

plot_seasonalfreq_2016

# Season: Spring
most_violation_spring_2016 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                          count(*) as Frequencies_of_Tickets 
                                          from bins_months_data_2016_sql  
                                          where bin_months = 'Spring'
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")

most_violation_spring_df_2016 <- collect(most_violation_spring_2016)
most_violation_spring_df_2016$year <- c(replicate(3,'2016'))
most_violation_spring_df_2016$Season <- c(replicate(3,'Spring'))
most_violation_spring_df_2016


# Results:
# 
#   Violation_Code Frequencies_of_Tickets year Season
# 1             21                 383448 2016 Spring
# 2             36                 374362 2016 Spring
# 3             38                 299439 2016 Spring


# Season: Summer
most_violation_summer_2016 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                          count(*) as Frequencies_of_Tickets 
                                          from bins_months_data_2016_sql  
                                          where bin_months = 'Summer'
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")


most_violation_summer_df_2016 <- collect(most_violation_summer_2016)
most_violation_summer_df_2016$year <- c(replicate(3,'2016'))
most_violation_summer_df_2016$Season <- c(replicate(3,'Summer'))
most_violation_summer_df_2016

# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             21                 358896 2016 Summer
# 2             38                 255600 2016 Summer
# 3             14                 200608 2016 Summer



# Season: Fall
most_violation_fall_2016 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                        count(*) as Frequencies_of_Tickets 
                                        from bins_months_data_2016_sql  
                                        where bin_months = 'Fall'
                                        group by Violation_Code
                                        order by count(*) desc
                                        limit 3")

most_violation_fall_df_2016 <- collect(most_violation_fall_2016)
most_violation_fall_df_2016$year <- c(replicate(3,'2016'))
most_violation_fall_df_2016$Season <- c(replicate(3,'Fall'))
most_violation_fall_df_2016


# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             36                 438320 2016   Fall
# 2             21                 395020 2016   Fall
# 3             38                 303387 2016   Fall


# Season: Winter
most_violation_winter_2016 <- SparkR::sql("select Violation_Code, 
                                          count(*) as Frequencies_of_Tickets 
                                          from bins_months_data_2016_sql  
                                          where bin_months = 'Winter'
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")

most_violation_winter_df_2016 <- collect(most_violation_winter_2016)
most_violation_winter_df_2016$year <- c(replicate(3,'2016'))
most_violation_winter_df_2016$Season <- c(replicate(3,'Winter'))
most_violation_winter_df_2016


# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             21                 359905 2016 Winter
# 2             36                 314765 2016 Winter
# 3             38                 268409 2016 Winter


###combining the violation code results for 2016
most_violation_df_2016<-rbind(most_violation_winter_df_2016,most_violation_summer_df_2016,most_violation_fall_df_2016,most_violation_spring_df_2016)
most_violation_df_2016

###Plotting graph for finding the top 3 Violation Codes for each Season of year 2016
plot_top_3_Violation_Code_Season_2016 <- ggplot(most_violation_df_2016,aes(x=Season,y=Frequencies_of_Tickets )) + 
                                         geom_bar(aes(fill=factor(Violation_Code)),stat="identity")+theme_bw() +xlab('Season') + ylab('Count of tickets') + 
                                         ggtitle('Year 2016 : Top 3 violation codes for each Season')

plot_top_3_Violation_Code_Season_2016


### Year: 2017

month_data_2017 <- SparkR::sql("select `Violation Code` as Violation_Code,
                               TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP)) as Date,  
                               MONTH(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as Month 
                               from parking_2017_sql")

createOrReplaceTempView(month_data_2017, "month_data_2017_sql")


distinct_month_data_2017 <- SparkR::sql("select distinct MONTH(TO_DATE(CAST(UNIX_TIMESTAMP(`Issue Date`, 'MM/dd/yyyy') AS TIMESTAMP))) as distinct_month
                                        from parking_2017_sql")


head(month_data_2017, 5)


# Results:

#    Violation_Code   Date     Month
# 1             20 2016-09-12     9
# 2             70 2016-08-24     8
# 3             17 2017-02-05     2
# 4             74 2017-05-20     5
# 5             21 2017-02-09     2


distinct_month_data_df_2017 <- collect(distinct_month_data_2017)
distinct_month_data_df_2017

# Results:

#         distinct_month
# 1              12
# 2               1
# 3               6
# 4               3
# 5               5
# 6               9
# 7               4
# 8               8
# 9               7
# 10             10
# 11             11
# 12              2


# The distinct month dataframe was just to check for invalid months, if any. But we found no errors and
# are good to go for binning the months column for seasons.


# Binning months into different seasons
bins_months_data_2017 <- SparkR::sql("select Violation_Code, Date, Month, \
                                     case when (month >= 3 and month <= 5) then 'Spring'\
                                     when (month >= 6 and month <= 8) then 'Summer'\
                                     when (month >= 9 and month <= 11) then 'Fall'\
                                     else 'Winter' end as bin_months from month_data_2017_sql")

createOrReplaceTempView(bins_months_data_2017, "bins_months_data_2017_sql")


head(bins_months_data_2017)

# Results:

#    Violation_Code   Date     Month   bin_months
# 1             20 2016-09-12     9       Fall
# 2             70 2016-08-24     8     Summer
# 3             17 2017-02-05     2     Winter
# 4             74 2017-05-20     5     Spring
# 5             21 2017-02-09     2     Winter
# 6             20 2016-08-30     8     Summer



### Frequencies of Tickets For Each Season is:
seasonal_frequency_2017 <- SparkR::sql("select bin_months as Seasons, 
                                       count(*) as Frequencies_of_Tickets 
                                       from bins_months_data_2017_sql 
                                       group by bin_months")


seasonal_frequency_df_2017 <- collect(seasonal_frequency_2017)
seasonal_frequency_df_2017$year<-c(replicate(4,'2017'))
seasonal_frequency_df_2017

# Results:

#   Seasons Frequencies_of_Tickets year
# 1  Spring                2873383 2017
# 2  Summer                2353920 2017
# 3    Fall                2829224 2017
# 4  Winter                2483036 2017


# Plotting Seasonal Frequency for year 2017
plot_seasonalfreq_2017 <- ggplot(seasonal_frequency_df_2017,aes(Seasons,Frequencies_of_Tickets))+ geom_col(width = 0.5) + 
                          geom_text(aes(label = Frequencies_of_Tickets), vjust = -0.25) + xlab('Seasons') + ylab('Count of tickets') + 
                          ggtitle('Year 2017 : Frequency of tickets per season') + theme_grey()

plot_seasonalfreq_2017

### 3 most violations for each season


# Season: Spring
most_violation_spring_2017 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                          count(*) as Frequencies_of_Tickets 
                                          from bins_months_data_2017_sql  
                                          where bin_months = 'Spring'
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")


most_violation_spring_df_2017 <- collect(most_violation_spring_2017)
most_violation_spring_df_2017$year <- c(replicate(3,'2017'))
most_violation_spring_df_2017$Season <- c(replicate(3,'Spring'))
most_violation_spring_df_2017

# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             21                 402424 2017 Spring
# 2             36                 344834 2017 Spring
# 3             38                 271167 2017 Spring


# Season: Summer
most_violation_summer_2017 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                          count(*) as Frequencies_of_Tickets 
                                          from bins_months_data_2017_sql  
                                          where bin_months = 'Summer'
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")


most_violation_summer_df_2017 <- collect(most_violation_summer_2017)
most_violation_summer_df_2017$year <- c(replicate(3,'2017'))
most_violation_summer_df_2017$Season <- c(replicate(3,'Summer'))
most_violation_summer_df_2017

# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             21                 378699 2017 Summer
# 2             38                 235725 2017 Summer
# 3             14                 207495 2017 Summer


# Season: Fall
most_violation_fall_2017 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                        count(*) as Frequencies_of_Tickets 
                                        from bins_months_data_2017_sql  
                                        where bin_months = 'Fall'
                                        group by Violation_Code
                                        order by count(*) desc
                                        limit 3")


most_violation_fall_df_2017 <- collect(most_violation_fall_2017)
most_violation_fall_df_2017$year <- c(replicate(3,'2017'))
most_violation_fall_df_2017$Season <- c(replicate(3,'Fall'))
most_violation_fall_df_2017


# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             36                 456046 2017   Fall
# 2             21                 357257 2017   Fall
# 3             38                 283816 2017   Fall


# Season: Winter
most_violation_winter_2017 <- SparkR::sql("select Violation_Code as Violation_Code, 
                                          count(*) as Frequencies_of_Tickets 
                                          from bins_months_data_2017_sql  
                                          where bin_months = 'Winter'
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")

most_violation_winter_df_2017 <- collect(most_violation_winter_2017)
most_violation_winter_df_2017$year <- c(replicate(3,'2017'))
most_violation_winter_df_2017$Season <- c(replicate(3,'Winter'))
most_violation_winter_df_2017

# Results:

#   Violation_Code Frequencies_of_Tickets year Season
# 1             21                 362016 2017 Winter
# 2             36                 359338 2017 Winter
# 3             38                 259710 2017 Winter

###combining the violation code results for 2017
most_violation_df_2017<-rbind(most_violation_winter_df_2017,most_violation_summer_df_2017,most_violation_fall_df_2017,most_violation_spring_df_2017)
most_violation_df_2017

###Plotting graph for finding the top 3 Violation Codes for each Season of year 2017
plot_top_3_Violation_Code_Season_2017 <- ggplot(most_violation_df_2017,aes(x=Season,y=Frequencies_of_Tickets )) + 
                                         geom_bar(aes(fill=factor(Violation_Code)),stat="identity")+theme_bw() +xlab('Season') + ylab('Count of tickets') + 
                                         ggtitle('Year 2017 : Top 3 violation codes for each Season')

plot_top_3_Violation_Code_Season_2017


###Combining the data of all the three years and plotting graph for making comparison of count of violation codes per seasons
most_violation_df<-rbind(most_violation_df_2015,most_violation_df_2016,most_violation_df_2017)
plot_most_violation_df<-ggplot(most_violation_df,aes(bins_months,count))+geom_bar((Violation_code),stat = "identity")+facet_wrap(~year)+
  xlab('Seasons 2017')+ylab('count of tickets')
+ggtitle("Frequency of violation code per seasons for 2017")


###Combining the data of all seasons together for all years : 2015,2016 and 2017
seasonal_frequency_df_all <- rbind(seasonal_frequency_df_2015, seasonal_frequency_df_2016, seasonal_frequency_df_2017)
seasonal_frequency_df_all

top_3_Violation_Codes_Seasons <- rbind(most_violation_df_2015, most_violation_df_2016, most_violation_df_2017)
top_3_Violation_Codes_Seasons


###Plotting graph for Seasonal Frequency for all years
plot_Seasonal_Frequency <- ggplot(seasonal_frequency_df_all, aes(Seasons, Frequencies_of_Tickets)) + geom_col() + 
                           facet_grid(~year)+ xlab('Seasons') + ylab('Tickets Issued') +
                           ggtitle('Seasonal Frequency for all Years') + theme(axis.text.x = element_text(angle = 90, vjust = 0.5))

plot_Seasonal_Frequency


###Plotting graph for finding the top 3 violation COdes per Season for all years
plot_top_3_Violation_Codes_Seasons <- ggplot(top_3_Violation_Codes_Seasons, aes(factor(Violation_Code), Frequencies_of_Tickets)) + geom_col() + 
                                      facet_grid(year~Season)+ xlab('Violation Code') + ylab('Tickets Issued') +
                                      ggtitle('Top 3 violation codes per Season for all Years') + theme(axis.text.x = element_text(angle = 90, vjust = 0.5))

plot_top_3_Violation_Codes_Seasons


#############################################################################################################################

###### 7) The fines collected from all the parking violation constitute a revenue source 
######    for the NYC police department. 
######    Let?s take an example of estimating that for the 3 most commonly occurring codes.

### Find total occurrences of the 3 most common violation codes


# Year: 2015


top_3_violation_codes_2015 <- SparkR::sql("select `Violation Code` as Violation_Code, 
                                          count(*) as Occurrences
                                          from parking_2015_sql
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")

createOrReplaceTempView(top_3_violation_codes_2015, "top_3_violation_codes_2015_sql")


top_3_violation_codes_df_2015 <- collect(top_3_violation_codes_2015)
top_3_violation_codes_df_2015$year <- c(replicate(3, '2015'))
top_3_violation_codes_df_2015

# Output:

# Violation_Code Occurrences year
# 1             21     1469228 2015
# 2             38     1305007 2015
# 3             14      908418 2015


# Fines for the top 3 codes are as follows: (taken from official NYC website as suggested in the
# guidelines.) We have added a column as Average of two, since as per guidelines, thats what we 
# have to take into consideration for further calculations.

###  Code ----- Fine for High Density Location ----- Fine For Other Areas ----- Average of two

#     21                   $65                             $45                      $55
#     38                   $65                             $35                      $50
#     14                   $115                            $115                     $115


###### Using this information 
###### Find the total amount collected for all of the fines. 
###### State the code which has the highest total collection.

highest_total_collection_2015 <- SparkR::sql("select Violation_Code, Occurrences, \ 
                                             case when Violation_Code = 21 then Occurrences * 55\
                                             when Violation_Code = 38 then Occurrences * 50\
                                             else Occurrences * 115 end as Total_Fines_Collected 
                                             from top_3_violation_codes_2015_sql
                                             order by Total_Fines_Collected desc")


highest_total_collection_df_2015 <- collect(highest_total_collection_2015)
highest_total_collection_df_2015$year<-c(replicate(3,'2015'))
highest_total_collection_df_2015

# Output:

#   Violation_Code Occurrences Total_Fines_Collected year
# 1             14      908418             104468070 2015
# 2             21     1469228              80807540 2015
# 3             38     1305007              65250350 2015


# Plotting the top 3 Highest Collections for year 2015
plot_highest_total_collection_df_2015 <-ggplot(highest_total_collection_df_2015,aes(factor(Violation_Code),Total_Fines_Collected))+geom_col(width = 0.5)+
                                        geom_text(aes(label = Total_Fines_Collected), vjust = -0.25) + 
                                        xlab("Violation code") + ylab("Total Fines Collected") + ggtitle("Year 2015: Highest fine collection per violation code")

plot_highest_total_collection_df_2015

# Code 14 has the highest collection of $104468070 from the top 3 most occurring codes for the year 2015.


# Year: 2016


top_3_violation_codes_2016 <- SparkR::sql("select `Violation Code` as Violation_Code, 
                                          count(*) as Occurrences
                                          from parking_2016_sql
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")

createOrReplaceTempView(top_3_violation_codes_2016, "top_3_violation_codes_2016_sql")


top_3_violation_codes_df_2016 <- collect(top_3_violation_codes_2016)
top_3_violation_codes_df_2016$year <- c(replicate(3, '2016'))
top_3_violation_codes_df_2016

# Output:

#   Violation_Code Occurrences year
# 1             21     1497269 2016
# 2             36     1232952 2016
# 3             38     1126835 2016


# Fines for the top 3 codes are as follows: (taken from official NYC website as suggested in the
# guidelines.) We have added a column as Average of two, since as per guidelines, thats what we 
# have to take into consideration for further calculations.

###  Code ----- Fine for High Density Location ----- Fine For Other Areas ----- Average of two

#     21                   $65                             $45                      $55
#     36                   $50                             $50                      $50
#     38                   $65                             $35                      $50


###### Using this information 
###### Find the total amount collected for all of the fines. 
###### State the code which has the highest total collection.

highest_total_collection_2016 <- SparkR::sql("select Violation_Code, Occurrences, \ 
                                             case when Violation_Code = 21 then Occurrences * 55\
                                             when Violation_Code = 38 then Occurrences * 50\
                                             else Occurrences * 50 end as Total_Fines_Collected 
                                             from top_3_violation_codes_2016_sql
                                             order by Total_Fines_Collected desc")


highest_total_collection_df_2016 <- collect(highest_total_collection_2016)
highest_total_collection_df_2016$year<-c(replicate(3,'2016'))
highest_total_collection_df_2016


# Output:

#   Violation_Code Occurrences Total_Fines_Collected year
# 1             21     1497269              82349795 2016
# 2             36     1232952              61647600 2016
# 3             38     1126835              56341750 2016


# Plotting the top 3 Highest Collections for year 2016
plot_highest_total_collection_df_2016 <-ggplot(highest_total_collection_df_2016, aes(factor(Violation_Code),Total_Fines_Collected))+geom_col(width = 0.5)+
                                        geom_text(aes(label = Total_Fines_Collected), vjust = -0.25) + 
                                        xlab("Violation code") + ylab("Total Fines Collected") + ggtitle("Year 2016: Highest fine collection per violation code")

plot_highest_total_collection_df_2016


# Code 21 has the highest collection of $82349795 from the top 3 most occurring codes for the year 2016.



# Year: 2017


top_3_violation_codes_2017 <- SparkR::sql("select `Violation Code` as Violation_Code, 
                                          count(*) as Occurrences
                                          from parking_2017_sql
                                          group by Violation_Code
                                          order by count(*) desc
                                          limit 3")

createOrReplaceTempView(top_3_violation_codes_2017, "top_3_violation_codes_2017_sql")


top_3_violation_codes_df_2017 <- collect(top_3_violation_codes_2017)
top_3_violation_codes_df_2017$year <- c(replicate(3, '2017'))
top_3_violation_codes_df_2017

# Output:

#   Violation_Code Occurrences year
# 1             21     1500396 2017
# 2             36     1345237 2017
# 3             38     1050418 2017

# Fines for the top 3 codes are as follows: (taken from official NYC website as suggested in the
# guidelines.) We have added a column as Average of two, since as per guidelines, thats what we 
# have to take into consideration for further calculations.

###  Code ----- Fine for High Density Location ----- Fine For Other Areas ----- Average of two

#     21                   $65                             $45                      $55
#     36                   $50                             $50                      $50
#     38                   $65                             $35                      $50


###### Using this information 
###### Find the total amount collected for all of the fines. 
###### State the code which has the highest total collection.

highest_total_collection_2017 <- SparkR::sql("select Violation_Code, Occurrences, \ 
                                             case when Violation_Code = 21 then Occurrences * 55\
                                             when Violation_Code = 38 then Occurrences * 50\
                                             else Occurrences * 50 end as Total_Fines_Collected 
                                             from top_3_violation_codes_2017_sql
                                             order by Total_Fines_Collected desc")


highest_total_collection_df_2017 <- collect(highest_total_collection_2017)
highest_total_collection_df_2017$year<-c(replicate(3,'2017'))
highest_total_collection_df_2017

# Output:

# Violation_Code Occurrences Total_Fines_Collected year
# 1             21     1500396              82521780 2017
# 2             36     1345237              67261850 2017
# 3             38     1050418              52520900 2017



# Plotting the top 3 Highest Collections for year 2017
plot_highest_total_collection_df_2017 <- ggplot(highest_total_collection_df_2017, aes(factor(Violation_Code),Total_Fines_Collected))+geom_col(width = 0.5)+
                                         geom_text(aes(label = Total_Fines_Collected), vjust = -0.25) + 
                                         xlab("Violation code") + ylab("Total Fines Collected") + ggtitle("Year 2017: Highest fine collection per violation code")

plot_highest_total_collection_df_2017


# Code 21 has the highest collection of $82521780 from the top 3 most occurring codes.

###Combining the data of all the years for making comparison
highest_total_collection_df <- rbind(highest_total_collection_df_2015, highest_total_collection_df_2016, highest_total_collection_df_2017)
highest_total_collection_df

###Plotting the graph for comparing total fines collected for violation code per year
plot_highest_total_collection_df<-ggplot(highest_total_collection_df,aes(factor(Violation_Code),Total_Fines_Collected))+geom_col()+facet_wrap(~year) +
                                  xlab("Violation code") + ylab("Total Fines Collected") + ggtitle("Highest fine collection per violation code for each Fiscal Year")
plot_highest_total_collection_df

