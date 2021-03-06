# Data Engineering Coding Challenge - Thomas Ernste (Python 3.6)

# Documentation

## Language/ Modules used

I used Python 3.6 to write the source code. The modules I used for this challenge were csv, time, datetime, sys, and functools.

## Explanation of source code

This repository builds a pipeline to ingest streaming data from the the Securities and Exchange Commission's Electronic Data Gathering, Analysis and Retrieval (EDGAR) system and calculates how long any given user spends on EDGAR during a visit and how many documents that user requests during the session.

My approach to this task started with reading reading the data in using the csv module. I preserved the 'ip' address, the 'date', and the 'time' columns for performing the data engineering on this data. I then combined the 'date' and 'time' columns into a single column, 'date_time', and then used the datetime and time modules to convert the 'date_time' column into a Coordinated Universal Time (UTC).

Next, I parsed through each ip address one at a time using a Python dictionary, with the ip address as the dictionary key and the 'date_time' column as the value in the dictionary. If the values in the 'date_time' column for consecutive rows/requests for a given ip address in the data were NOT within the number of seconds in the inactivity_period.txt file, then the first of those rows/requests was added to a final Python list (this would eventually be the final list for the output data) and counted as a single visit with a duration of 1 second. Noteably, duration is calculated by taking the time difference between the first and last requests by an ip address in a single visit, plus 1 to account for the fact that the challenge asks for the session durations to be time-inclusive. 

If the values in the 'date_time' column for consecutive rows for a given request by an ip address in the data were within the number of seconds in the inactivity_period.txt file, then those rows/requests were added together and counted as part of a single visit by that user. If there is eventually a break between weblog requests by a user of more than the number of seconds in the inactivity file, then the row with multiple requests by a user is then added to the final Python list and counted as single visit with the multiple number of requests counted and the duration calculated -- again -- as the time difference between the first and last requests in that visit by the ip address -- again -- plus 1 to account for the fact that the challenge asks for the session durations to be time-inclusive.

Finally, because I iterated through users rather than  by seconds, the rows in the resulting 2D array at this time are not sorted properly. Therefore, my code includes a function that sorts first in descending order by the start_time, but for a unique condition the rows are conditionally sorted in descending order by end_time. Specifically, the code adds a condition that if, for example, we have two ip addresses, and the first has an earlier start_time than the second, but the second has an end_time that occurs before the expiration of the inactivity value for the first ip address, then the second ip address will appear earlier in the output under such conditions.

The output list for the code successfully completes the challenge to calculate how long each user spends on EDGAR per visit and the number of requests per visit. With this output dataset, data scientists and others in an organization could assess the streaming web traffic behavior on the SEC's EDGAR system.

