# FlinkProject
This project consists in writing a Flink application in scala, that gets events from 2 Kafka topics (clicks and displays) and outputs some anomalies detected from chosen patterns. We chose to get a unique datastream containing both the click and display events. The datastream enters three different pipelines, each one dedicated to the detection of one type of anomaly. We are looking for anormal click activity for a user (i.e. an ip). We detect anomalies by computing three different measures (one for each anomaly) by ip, over a certain window of time. We decribe our use of windows, timestamps and watermarks later on. 

## Measures and Thresholds
### Click Through Rate (CTR)
The first measure we compute is the CTR = number of clicks / number of displays, by ip. It is known that a "normal" CTR is around 10%. Thus we defined a CTR anomaly as a CTR > 30%. What's more, we had to consider an exception : when the number of clicks was > 0 and the number of displays was 0. As we could not divide by 0, we had to choose an arbitrary value to assign the CTR. Observing clicks and no display is clearly an anomaly, we chose to assign 1.0 as the CTR value : the threshold being at 30%, a CTR at 1.0 would be detected as an anomaly. 

This measures detects anormal repartition of clicks and displays within a period of time, for a unique ip.

### Average Time between two clicks (AVG)
Our second measure is the AVG. For an ip, we compute the average of seconds between two consecutive clicks (within a window). We wanted to detect periods where the clicks were too frequent to have been performed by humans. We defined an anomaly as an AVG lower than a threshold (set at 2 seconds) BUT greater than 0. An AVG at zero means that there only was 1 click, so it should not be detected as an anomaly.

### Variance of Time between two clicks (VAR)
Our last measure is VAR. For an ip, we compute the variance of the seconds between two consecutive clicks. This measure allows us to detect clicks that are too regular : a variance to 0 means that there is always the same number of seconds between two clicks, which seems anormal. We defined an anomaly as a VAR lower than a threshold (set at 1 second) and greater or equal to zero. As we explained, a variance to zero is anormal. However, if we only have 2 clicks then the variance is 0 also but is not an anomaly. Thus we chose to set the VAR to -1 in this case, this way, it is not labeled as an anomaly.

## Windows, Timestamps and Watermarks

## Choix à expliquer :
3 types d’anomalies :
- CTR (on calcule le nb de clicks / nb de displays par ip par window) => anomaly = CTR > seuil (0,3)
- AverageTimeDiff (temps moyen entre 2 clicks consécutifs par ip par window) => anomaly =  (car si = 0, alors ça veut dire qu’il y a 1 seul click) 0<AVG < seuil (2sec) 
- VarianceTimeDiff (variance du temps entre 2 clicks consécutifs par ip par window) => anomaly = 0<= VAR < seuil (?) (on définit dans notre calcul de la variance que la variance est négative si le nb de timediff est = 1, ainsi var=0 seulement si on a plus d’un click et que tous sont arrivés avec autant de diff de temps)

Sliding windows (définir la longueur d’une window et du slide entre deux windows)
