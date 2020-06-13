# FlinkProject
TODO

## Choix à expliquer :
3 types d’anomalies :
CTR (on calcule le nb de clicks / nb de displays par ip par window) => anomaly = CTR > seuil (0,3)
AverageTimeDiff (temps moyen entre 2 clicks consécutifs par ip par window) => anomaly =  (car si = 0, alors ça veut dire qu’il y a 1 seul click) 0<AVG < seuil (2sec) 
VarianceTimeDiff (variance du temps entre 2 clicks consécutifs par ip par window) => anomaly = 0<= VAR < seuil (?) (on définit dans notre calcul de la variance que la variance est négative si le nb de timediff est = 1, ainsi var=0 seulement si on a plus d’un click et que tous sont arrivés avec autant de diff de temps)

Sliding windows (définir la longueur d’une window et du slide entre deux windows)
