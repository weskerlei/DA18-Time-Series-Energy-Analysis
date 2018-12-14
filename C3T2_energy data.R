##Import libraries
library(readr)
library(dplyr)
library(RMySQL)
library(ggplot2)

##Parallel Computing##
#--- for Win ---#
library(doParallel) 

# Check number of cores and workers available 
detectCores()
getDoParWorkers()
cl <- makeCluster(detectCores()-1, type='PSOCK')
registerDoParallel(cl)

## Create a database connection## 
con = dbConnect(MySQL(), user='deepAnalytics',
                password='Sqltask1234!', dbname='dataanalytics2018',
                host='data-analytics-2018.cbrosir2cswx.us-east-1.rds.amazonaws.com')

##List the tables contained in the database
dbListTables(con)

##List attributes contained in a table
dbListFields(con, "yr_2006")

##query specific attributes from tables yr_2006 to yr_2010
yr_2006 <- dbGetQuery(con, "SELECT Date, Time,Sub_metering_1, Sub_metering_2, Sub_metering_3 FROM yr_2006")
yr_2007 <- dbGetQuery(con, "SELECT Date, Time,Sub_metering_1, Sub_metering_2, Sub_metering_3 FROM yr_2007")
yr_2008 <- dbGetQuery(con, "SELECT Date, Time,Sub_metering_1, Sub_metering_2, Sub_metering_3 FROM yr_2008")
yr_2009 <- dbGetQuery(con, "SELECT Date, Time,Sub_metering_1, Sub_metering_2, Sub_metering_3 FROM yr_2009")
yr_2010 <- dbGetQuery(con, "SELECT Date, Time,Sub_metering_1, Sub_metering_2, Sub_metering_3 FROM yr_2010")

##inspect data frame from query
summary(yr_2006)
summary(yr_2007)
summary(yr_2008)
summary(yr_2009)
summary(yr_2010)

head(yr_2006)
head(yr_2007)
head(yr_2008)
head(yr_2009)
head(yr_2010)

tail(yr_2006)
tail(yr_2007)
tail(yr_2008)
tail(yr_2009)
tail(yr_2010)

##combine yr07-09
yr_789 <- bind_rows(yr_2007, yr_2008, yr_2009)
summary(yr_789)
head(yr_789)
tail(yr_789)

## Combine Date and Time attribute values in a new attribute column
yr_789 <-cbind(yr_789,paste(yr_789$Date,yr_789$Time), stringsAsFactors=FALSE)

## Give the new attribute in the 6th column a header name
colnames(yr_789)[6] <- "DateTime"

## Move the DateTime attribute within the dataset
yr_789 <- yr_789[,c(ncol(yr_789), 1:(ncol(yr_789)-1))]

## Convert DateTime from character to POSIXct 
yr_789$DateTime <- as.POSIXct(yr_789$DateTime, "%Y/%m/%d %H:%M:%S")

## Add the time zone - Europe/Paris
summary(yr_789)
head(yr_789)
attr(yr_789$DateTime, "tzone") <- "Europe/Paris"
head(yr_789)

## substrate an hour from datetime
yr_789$DateTime <- yr_789$DateTime - 3600
attr(yr_789$DateTime, "tzone")
summary(yr_789)

##load package Lubridate
library(lubridate)

##creat "year" attribute and append to dataframe
yr_789$year <- year(yr_789$DateTime)

##creat quarter, month, week, weekday, day, hour, minute attributes and append to dataframe
yr_789$quarter <- quarter(yr_789$DateTime, with_year = FALSE, fiscal_start = 1)
yr_789$month <- month(yr_789$DateTime)
yr_789$week <- week(yr_789$DateTime)
yr_789$weekday <- weekdays.Date(yr_789$DateTime, abbreviate = TRUE)
yr_789$day <- day(yr_789$DateTime)
yr_789$hour <- hour(yr_789$DateTime)
yr_789$minute <- minute(yr_789$DateTime)

summary(yr_789)
head(yr_789)
str(yr_789)

## Plot all of sub-meter 1
plot(yr_789$Sub_metering_1)

## Subset the second week of 2008 - All Observations
houseWeek <- filter(yr_789, year == 2008 & week == 2)
## Plot subset houseWeek
plot(houseWeek$Sub_metering_1)

##Install package plotly
library(plotly)

## Subset the 9th day of January 2008 - All observations
houseDay <- filter(yr_789, year == 2008 & month == 1 & day == 9)
summary(houseDay)
head(houseDay)
## Plot sub-meter 1,2,3 of houseDay
plot_ly(houseDay, x = ~houseDay$DateTime, y = ~houseDay$Sub_metering_1, name = 'Kitchen', type = 'scatter', 
        mode = 'lines') %>%
  add_trace(y = ~houseDay$Sub_metering_2, name = 'Laundry Room', mode = 'lines') %>%
  add_trace(y = ~houseDay$Sub_metering_3, name = 'Water Heater & AC', mode = 'lines') %>%
  layout(title = "Power Consumption January 9th, 2008",
         xaxis = list(title = "Time"),
         yaxis = list (title = "Power (watt-hours)"))

## Subset the 9th day of January 2008 - 10 Minute frequency
houseDay9_Sep_08 <- filter(yr_789, year == 2008 & month == 9 & day == 2 & 
                       (minute == 0 | minute == 10 | minute == 20 | minute == 30 | 
                          minute == 40 | minute == 50))

houseDay10 <- filter(yr_789, year == 2008 & month == 1 & day == 9 & 
                       (minute == 0 | minute == 10 | minute == 20 | minute == 30 | 
                          minute == 40 | minute == 50))

summary(houseDay10)

## Plot day - 10 Minute frequency
plot_ly(houseDay10, x = ~houseDay10$DateTime, y = ~houseDay10$Sub_metering_1, name = 'Kitchen', 
        type = 'scatter', mode = 'lines') %>%
  add_trace(y = ~houseDay10$Sub_metering_2, name = 'Laundry Room', mode = 'lines') %>%
  add_trace(y = ~houseDay10$Sub_metering_3, name = 'Water Heater & AC', mode = 'lines') %>%
  layout(title = "Power Consumption January 9th, 2008",
         xaxis = list(title = "Time"),
         yaxis = list (title = "Power (watt-hours)"))

plot_ly(houseDay9_Sep_08, x = ~houseDay9_Sep_08$DateTime, y = ~houseDay9_Sep_08$Sub_metering_1, name = 'Kitchen', 
        type = 'scatter', mode = 'lines') %>%
  add_trace(y = ~houseDay9_Sep_08$Sub_metering_2, name = 'Laundry Room', mode = 'lines') %>%
  add_trace(y = ~houseDay9_Sep_08$Sub_metering_3, name = 'Water Heater & AC', mode = 'lines') %>%
  layout(title = "Power Consumption Sep 9th, 2008",
         xaxis = list(title = "Time"),
         yaxis = list (title = "Power (watt-hours)"))


## Subset the week 2 of January 2008 - 30 Minute frequency
houseWeek2_30 <- filter(yr_789, year == 2008 & month == 2 & week == 6 & (minute == 0 | minute == 30))
summary(houseWeek2_30)
head(houseWeek2_30)

## Plot Feb  wk6, sub-meter 1, 2 and 3 with title, legend and labels - 30 Minute frequency
plot_ly(houseWeek2_30, x = ~houseWeek2_30$DateTime, y = ~houseWeek2_30$Sub_metering_1, name = 'Kitchen', 
        type = 'scatter', mode = 'lines') %>%
  add_trace(y = ~houseWeek2_30$Sub_metering_2, name = 'Laundry Room', mode = 'lines') %>%
  add_trace(y = ~houseWeek2_30$Sub_metering_3, name = 'Water Heater & AC', mode = 'lines') %>%
  layout(title = "Power Consumption Feb 5-11, 2008",
         xaxis = list(title = "Time"),
         yaxis = list (title = "Power (watt-hours)"))

## Subset the month of Jan, 2008 - 30 Minute frequency
houseMonth_Jan08 <- filter(yr_789, year == 2008 & month == 1 & (hour == 0 | hour ==  3 | hour == 6 |
                                                                hour == 9 | hour ==  12 | hour == 15 |
                                                                hour == 18 | hour ==  21) & minute == 0)
houseMonth_Jun08 <- filter(yr_789, year == 2008 & month == 6 & (hour == 0 | hour ==  3 | hour == 6 |
                                                                  hour == 9 | hour ==  12 | hour == 15 |
                                                                  hour == 18 | hour ==  21) & minute == 0)

summary(houseMonth_Jan08)
head(houseMonth_Jan08)

## Plot Monthly, Jan and June
plot_ly(houseMonth_Jan08, x = ~houseMonth_Jan08$DateTime, y = ~houseMonth_Jan08$Sub_metering_1, name = 'Kitchen', 
        type = 'scatter', mode = 'lines') %>%
  add_trace(y = ~houseMonth_Jan08$Sub_metering_2, name = 'Laundry Room', mode = 'lines') %>%
  add_trace(y = ~houseMonth_Jan08$Sub_metering_3, name = 'Water Heater & AC', mode = 'lines') %>%
  layout(title = "Power Consumption Jan, 2008",
         xaxis = list(title = "Time"),
         yaxis = list (title = "Power (watt-hours)"))

plot_ly(houseMonth_Jun08, x = ~houseMonth_Jun08$DateTime, y = ~houseMonth_Jun08$Sub_metering_1, name = 'Kitchen', 
        type = 'scatter', mode = 'lines') %>%
  add_trace(y = ~houseMonth_Jun08$Sub_metering_2, name = 'Laundry Room', mode = 'lines') %>%
  add_trace(y = ~houseMonth_Jun08$Sub_metering_3, name = 'Water Heater & AC', mode = 'lines') %>%
  layout(title = "Power Consumption Jun, 2008",
         xaxis = list(title = "Time"),
         yaxis = list (title = "Power (watt-hours)"))

## Subset the year of 2008 - day frequency
houseYear_08 <- filter(yr_789, year == 2008 & hour == 12 & minute == 0)

plot_ly(houseYear_08, x = ~houseYear_08$DateTime, y = ~houseYear_08$Sub_metering_1, name = 'Kitchen', 
        type = 'scatter', mode = 'lines') %>%
  add_trace(y = ~houseYear_08$Sub_metering_2, name = 'Laundry Room', mode = 'lines') %>%
  add_trace(y = ~houseYear_08$Sub_metering_3, name = 'Water Heater & AC', mode = 'lines') %>%
  layout(title = "Power Consumption of 2008",
         xaxis = list(title = "Time"),
         yaxis = list (title = "Power (watt-hours)"))

## Subset to one observation per week on Mondays at 8:00pm for 2007, 2008 and 2009
house070809weekly <- filter(yr_789, weekday == 'Mon' & hour == 20 & minute == 0)
## Subset to 1 observation per day for 2007, 2008 and 2009
house070809daily <- filter(yr_789, hour == 18 & minute == 0)


## Create TS object with SubMeter3 - weekly
tsSM3_070809weekly <- ts(house070809weekly$Sub_metering_3, frequency=52, start=c(2007,1))
## Create TS object with SubMeter1 and 2 - daily
tsSM1_070809daily <- ts(house070809daily$Sub_metering_1, frequency=365, start=c(2007,1))
tsSM2_070809daily <- ts(house070809daily$Sub_metering_2, frequency=365, start=c(2007,1))

## Plot sub-meter 3 - autoplot
library(ggplot2)
library(ggfortify)
autoplot(tsSM3_070809weekly, ts.colour = 'red', xlab = "Time", ylab = "Watt Hours", main = "Sub-meter 3")
autoplot(tsSM1_070809daily, ts.colour = 'red', xlab = "Time", ylab = "Watt Hours", main = "Sub-meter 1")
autoplot(tsSM2_070809daily, ts.colour = 'red', xlab = "Time", ylab = "Watt Hours", main = "Sub-meter 2")
## Plot sub-meter 3 with plot.ts
plot.ts(tsSM3_070809weekly)

## Apply time series linear regression to the sub-meter 1,2,3 ts object
library(forecast)
fitSM3 <- tslm(tsSM3_070809weekly ~ trend + season) 
summary(fitSM3)
fitSM1 <- tslm(tsSM1_070809daily ~ trend + season) 
summary(fitSM1)
fitSM2 <- tslm(tsSM2_070809daily ~ trend + season) 
summary(fitSM2)

## Create the forecast for sub-meter 3. Forecast ahead 20 time periods 
fc_fitSM3 <- forecast(fitSM3, h=20, level=c(80,90))
## Create the forecast for sub-meter 1 and 2. Forecast ahead 562 time periods 
fc_fitSM1 <- forecast(fitSM1, h=562, level=c(80,90))
fc_fitSM2 <- forecast(fitSM2, h=562, level=c(80,90))

## Plot the forecast for sub-meter 3. 
plot(fc_fitSM3, main = 'Submeter 3 Forecast', ylim = c(0, 40), ylab= "Watt-Hours", xlab="Time")
plot(fc_fitSM1, main = 'Submeter 1 Forecast', ylim = c(0, 80), ylab= "Watt-Hours", xlab="Time")
plot(fc_fitSM2, main = 'Submeter 2 Forecast', ylim = c(0, 80), ylab= "Watt-Hours", xlab="Time")

## Decompose Sub-meter 1,2,3 into trend, seasonal and remainder
cpSM3_070809weekly <- decompose(tsSM3_070809weekly)
cpSM1_070809daily <- decompose(tsSM1_070809daily)
cpSM2_070809daily <- decompose(tsSM2_070809daily)

## Plot decomposed sub-meter 1,2,3 
plot(cpSM3_070809weekly)
plot(cpSM1_070809daily)
plot(cpSM2_070809daily)

## Check summary statistics for decomposed sub-meter 1,2,3 
summary(cpSM3_070809weekly$trend)
summary(cpSM3_070809weekly$seasonal)
summary(cpSM3_070809weekly$random)

summary(cpSM1_070809daily$trend)
summary(cpSM1_070809daily$seasonal)
summary(cpSM1_070809daily$random)

summary(cpSM2_070809daily$trend)
summary(cpSM2_070809daily$seasonal)
summary(cpSM2_070809daily$random)

## Subtract seasonal component from submeter 1,2,3 ts
tsSM3_070809adj <- tsSM3_070809weekly - cpSM3_070809weekly$seasonal
autoplot(tsSM3_070809adj)
tsSM1_070809adj <- tsSM1_070809daily - cpSM1_070809daily$seasonal
tsSM2_070809adj <- tsSM2_070809daily - cpSM2_070809daily$seasonal

## Holt Winter Exponential Smoothing and Plot for submeter 1,2,3
tsSM3_070809HW <- HoltWinters(tsSM3_070809adj, beta=FALSE, gamma = FALSE)
plot(tsSM3_070809HW, ylim = c(0,25), main = "Submeter 3 HoltWinters Smoothing")
tsSM1_070809HW <- HoltWinters(tsSM1_070809adj, beta=FALSE, gamma = FALSE)
plot(tsSM1_070809HW, ylim = c(0,50), main = "Submeter 1 HoltWinters Smoothing")
tsSM2_070809HW <- HoltWinters(tsSM2_070809adj, beta=FALSE, gamma = FALSE)
plot(tsSM2_070809HW, ylim = c(0,50), main = "Submeter 2 HoltWinters Smoothing")

##HoltWinters forecast and plot for submeter 1, 2, 3
fc_tsSM3_070809HW <- forecast(tsSM3_070809HW, h=25, level=c(10,25))
plot(fc_tsSM3_070809HW, ylim=c(0,25), ylab="Watt-Hours", xlab="Time", 
     main = "Submeter 3 HoltWinters Forecast")
plot(fc_tsSM3_070809HW, ylim=c(0,10), ylab="Watt-Hours", xlab="Time", 
     main = "Submeter 3 HoltWinters Forecast - 2010", start(2010))

fc_tsSM1_070809HW <- forecast(tsSM1_070809HW, h=176, level=c(10,25))
plot(fc_tsSM1_070809HW, ylim=c(0,50), ylab="Watt-Hours", xlab="Time", 
     main = "Submeter 1 HoltWinters Forecast")
plot(fc_tsSM1_070809HW, ylim=c(0,5), ylab="Watt-Hours", xlab="Time", 
     main = "Submeter 1 HoltWinters Forecast - 2010", start(2010))

fc_tsSM2_070809HW <- forecast(tsSM2_070809HW, h=176, level=c(10,25))
plot(fc_tsSM2_070809HW, ylim=c(0,50), ylab="Watt-Hours", xlab="Time", 
     main = "Submeter 2 HoltWinters Forecast")
plot(fc_tsSM2_070809HW, ylim=c(0,10), ylab="Watt-Hours", xlab="Time", 
     main = "Submeter 2 HoltWinters Forecast - 2010", start(2010))

