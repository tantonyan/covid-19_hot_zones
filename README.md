# COVID-19 Hot Zones and Trends
## Find trends using open datasets on population and COVID-19 cases in US

When reporting on number of confirmed cases it is helpful to also know what percent of the local population we are 
referring to. I combined the county level Census data to a time series COVID-19 data by Johns Hopkins CSSE to enhance 
it with percent of population based fields to help identify hot zones by looking at the most recent 30, 14 and 7 day 
confirmed cases as a percentage of the population per county.
I have also added fields to show what portion of the population will be confirmed to have the virus by 2021 if the 
spread was to continue at the same rate as observed for the three time ranges. My latest run (last date of 06/07/2020) 
shows that in more than 20 counties in the US the virus will reach to at least 15% of the population by 2021 with one 
of the three rates.

#### v0.1
Using [US Census data](https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/) 
(population estimates for 2019) per county with 
[2019 Novel Coronavirus COVID-19 (2019-nCoV) Data Repository by Johns Hopkins CSSE](https://github.com/CSSEGISandData/COVID-19)
compute confirmed cases as a percent of the county population as of the last reported date and within the following time periods:
* Last 30 days
* Last 14 days
* Last 7 days

For the same time ranges also compute increase/decrease rates and tangent.

Using the computed fields define `trend` as follows:
* `rising`: If the tangent is increasing across 30, 14 and 7 day windows
* `lowering`: If the tangent is decreasing across 30, 14 and 7 day windows
* `leveling` otherwise

Also forecast (`per_confirmed_by2021_` fields) the percent of the population that would be confirmed to have contracted the virus 
by 2021 using each of the three rates

Included files:
* Hot zones: [hot_zones_06-07-2020.csv](hot_zones_06-07-2020.csv)
* Databricks notebook: [hot_zones_pop_perc.dbc](hot_zones_pop_perc.dbc)
* Scala export of the notebook: [hot_zones_pop_perc.scala](hot_zones_pop_perc.scala)

##### TODO
Plot the data on a map (Databricks' map has a granularity of a state, not a county so a different tool will be necessary)
showing the current percent of population confirmed with the virus (size of a circle) and the trend (color?)
