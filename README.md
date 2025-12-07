# 507_groupproject_2025
group project

## Group members
1. Joe Li
2. Wan Yong Lin Huang
    - Part 1.2 Exploration
    - Part 2.3 Derived metric
    - Part 3.1 Individual analysis
    - Part 4.1 Flagging system
3. Yu Lin
4. Wei Wei Jiang
5. Chinyere Nwosu

## Part 1.2 Data Quality Assessment
### Methods
The MySQL database for the athlete sport information was accessed with sqlalchemy in python and using database credebtials that were securely stored. Then analyses was performed in python to answer specific questions regarding the database.

Individual python scripts were created to answer the questions for the assignment.
1. Perform a SQL query on the main table.
2. Return results as a pandas object.
3. Print a summary on the finding.

This approach avoids loading the whole table which reduces unnecessary memory usages and improves query speed.

### Key Findings
1. **Number of unique athletes**:
Using `count(distinct playername)`, we were able to identify **1287 unique athletes** in the database.

2. **Number of different sports/teams represented**
Using `count(distinct team)` shows **92 unique teams** in the database. This shows the amount of different sports within the database.

3. **Date range of available data**
By finding the `min(timestamp)` and `max(timestamp)` we were able to defind the time range that the data expanded from. The current data spans from **2018-10-15 19:27:41 to 2025-10-21 12:24:21** providing **7** years of data.

4. **Data source with the most records**
We grouped by `data_source` and counted by the number of rows per source. The source with the most records is **kinexon** with **4073754** records. This imbalance in records between sources show a biased towards using kinexon tracking for sport performance.

5. **Number of athletes with missing or invalid names**
We counted the rows where `playername` is `null` or matched any of the following `NA, N/A, na, n/a`. We found `0 rows` with missing or invalid player names.

6. **Athletes with data from multiple sources**
We examined players with data from multiple sources by grouping on `playername` and counting distinct `data_source` values for each athlete. Athletes with data from 2 or more sources `having count(distinct data_source) >=2` were added to total the count. There are **541 athletes** with data from multiple sources. This multi-sourcing of different athletes provides valuable data regarding the performance metrics of different tracking devices from different sources.

### Final thoughts
Overall, the database showed a broad amount of athletes and teams within the system. Furthermore, a large amount of data has been collected throughout multiple years with different tracking technologies. 