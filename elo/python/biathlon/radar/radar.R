install.packages("fmsb")
library(readxl)
library(fmsb)

ladies_all_k = read_excel("/Users/syverjohansen/ski/elo/python/ski/excel365/varladies_all_k.xlsx", 
                          sheet = "Sheet1", col_names = TRUE, na = "NA")

ladies = read_excel("/Users/syverjohansen/ski/elo/python/ski/radar/lady_values.xlsx", sheet="Sheet1", col_names=TRUE, na="NA")
ladies
ladies = data.frame(ladies[,2:10])
#which(ladies$Name=="Tatjana Kuznetsova")
#ladies$Name[649] = "Lene Pedersen2"
#ladies$Name[1912] = "Lilia Vasilieva2"
#ladies$Name[1470] = "Tatjana Kuznetsova2"
rownames(ladies) = ladies[,1]

ladies = ladies[,3:9]

ladies_list = c("Monika Skinder")
lady = ladies[ladies_list, ]
lady = rbind(rep(1,7), rep(.6, 7), lady)
radarchart(lady, title = ladies_list[1])




men = read_excel("/Users/syverjohansen/ski/elo/python/ski/radar/man_values.xlsx", sheet="Sheet1", col_names=TRUE, na="NA")
men = data.frame(men[,2:10])
#which(men$Name=="Gustaf Berglund")
#men$Name[2312] = "Alexander Schwarz2"
#men$Name[2363] = "Alexander Schwarz3"
#men$Name[1186] = "David Rees2"
#men$Name[604] = "Gunnar Eriksson2"
#men$Name[2034] = "Hannu Manninen2"
#men$Name[1893] = "Peter Klofutar2"
#men$Name[4032] = "Gustaf Berglund2"
rownames(men) = men[,1]
men = men[,3:9]




men_list = c("Graham Ritchie"
           )
man = men[men_list, ]
man
man = rbind(rep(1,7), rep(.6, 7), man)
radarchart(man, title = men_list[1])

