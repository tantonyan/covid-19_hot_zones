# COVID-19 Hot Zones and Trends
## Find trends using open datasets on population and COVID-19 cases in US
#### v0.1
Using [US Census data](https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/) 
(population estimates for 2019) per county with 
[2019 Novel Coronavirus COVID-19 (2019-nCoV) Data Repository by Johns Hopkins CSSE](https://github.com/CSSEGISandData/COVID-19)
compute confirmed cases as a percent of the county population for the present and the following time ranges:
* Last 30 days
* Last 14 days
* Last 7 days

For the same time range also compute increase/decrease rates and tangent.

Using the computed fields define `trend` as follows:
* `rising`: If the tangent is increasing across 30, 14 and 7 day windows
* `lowering`: If the tangent is decreasing across 30, 14 and 7 day windows
* `leveling` otherwise

Included files:
* Hot zones: [hot_zones_06-07-2020.csv](hot_zones_06-07-2020.csv)
* Databricks notebook: [hot_zones_pop_perc.dbc](hot_zones_pop_perc.dbc)
* Scala export of the notebook: [hot_zones_pop_perc.scala](hot_zones_pop_perc.scala)

##### TODO
Plot the data on a map (Databricks' map has a granularity of a state, not a county so a different tool will be necessary)
showing the current percent of population confirmed with the virus (size of a circle) and the trend (color?)
