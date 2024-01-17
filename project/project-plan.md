# Project Plan

## Title
Correlation analysis for Crime Data and Covid Cases

## Main Question

1. Does increase in covid cases in a particular region impact crime rate in the same region?
2. Analysing the Covid data to understand the trend of cases in order to understand hidden patterns, which might help in future.

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
With the recent pandemic that disturbed the whole world, it is critical to understand how diseases spread in order to help avoid future pandemics. COVID affected the majority of individuals and practically all institutions. It will be interesting to observe if the crime rate drops as well. It seems implausible to believe that crime would have decreased as well, yet it is possible. It would be interesting to observe what types of crime are more prevalent and in which areas. In addition, the association between the number of covid instances and the number of crimes committed in a certain location.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: LA County COVID Cases
* Metadata URL: https://catalog.data.gov/dataset/la-county-covid-cases
* Data URL: https://data.lacity.org/api/views/jsff-uc6b/rows.csv?accessType=DOWNLOAD
* Data Type: CSV

The dataset contains approx 24K records with the covid data for the state of california and LA County (total cases, deaths, new_cases, new_deaths). The data is recorded for each day.

### Datasource2: Crime Data from 2010 to Present
* Metadata URL: https://catalog.data.gov/dataset/crime-data-from-2020-to-present
* Data URL: https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD
* Data Type: CSV

The dataset contains approx 800K records with the crime data for the LA County (Data and Time, Area, Type of crime, Victim Age, Premisis details and location details). 

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. [Identify the Datasourse][i1] <br>
2. [Create poject plan][i2] <br>
3. [Understand and clean the dataset][i3] <br>
4. [Create Data Pipeline][i4] <br>
5. [Create a project report][i4]

[i1]: https://github.com/PremPrakashS/my-made-repo/issues/1
[i2]: https://github.com/PremPrakashS/my-made-repo/issues/2
[i3]: https://github.com/PremPrakashS/my-made-repo/issues/3
[i4]: https://github.com/PremPrakashS/my-made-repo/issues/6
[i5]: https://github.com/PremPrakashS/my-made-repo/issues/7

