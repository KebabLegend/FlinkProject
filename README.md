# FlinkProject
This project consists in writing a Flink application in scala, that gets events from 2 Kafka topics (clicks and displays) and outputs some anomalies detected from chosen patterns. We chose to get a unique datastream containing both the click and display events. The datastream enters three different pipelines, each one dedicated to the detection of one type of anomaly. We are looking for anormal click activity for a user (i.e. an uid). We detect anomalies by computing three different measures (one for each anomaly) by uid, over a certain window of time. 
We chose to use sliding windows of 30 minutes and a 5 minute trigger interval.

## Measures and Thresholds
### Click Through Rate (CTR)
The first measure we compute is the CTR = number of clicks / number of displays, by uid. It is known that a "normal" CTR is around 10%. Thus we defined a CTR anomaly as a CTR > 30%. 

This measures detects anormal repartition of clicks and displays within a period of time, for a unique uid.

### Average Time between two clicks (AVG)
Our second measure is the AVG. For a uid, we compute the average of seconds between two consecutive clicks (within a window). We wanted to detect periods where the clicks were too frequent to have been performed by humans. We defined an anomaly as an AVG lower than a threshold (set at 2 seconds) BUT greater than 0. An AVG at zero means that there only was 1 click, so it should not be detected as an anomaly.

### Average Delay between each click and the most recent corresponding display 
Our third measure detects whether if the user clicks too quickly after a display is done. We use the average of these delays in order to avoid detecting accidental clicks. 
We defined our threshold at 2 seconds, meaning that if the average time of click for the user is under 2 seconds after displays, we have an anomaly.



Intuitively, we thought about the following anomaly, but decided not to keep it because it was not happening in our datastream.
It can be found in comments of the presented code.
### Variance of Time between two clicks (VAR)
Our last measure is VAR. For a uid, we compute the variance of the seconds between two consecutive clicks. This measure allows us to detect clicks that are too regular : a variance to 0 means that there is always the same number of seconds between two clicks, which seems anormal. We defined an anomaly as a VAR lower than a threshold (set at 1 second) and greater or equal to zero. As we explained, a variance to zero is anormal. However, if we only have 2 clicks then the variance is 0 also but is not an anomaly. Thus we chose to set the VAR to -1 in this case, this way, it is not labeled as an anomaly.
