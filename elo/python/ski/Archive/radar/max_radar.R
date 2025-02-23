install.packages("fmsb")
library(readxl)
library(fmsb)

ladies_all_k = read_excel("/Users/syverjohansen/ski/elo/python/ski/excel365/varladies_all_k.xlsx", 
                          sheet = "Sheet1", col_names = TRUE, na = "NA")

ladies = read_excel("/Users/syverjohansen/ski/elo/python/ski/radar/max_lady_values.xlsx", sheet="Sheet1", col_names=TRUE, na="NA")
ladies
ladies = data.frame(ladies[,2:9])
which(ladies$Name=="Tatjana Kuznetsova")
ladies$Name[649] = "Lene Pedersen2"
ladies$Name[1912] = "Lilia Vasilieva2"
ladies$Name[1470] = "Tatjana Kuznetsova2"
rownames(ladies) = ladies[,1]

ladies = ladies[,2:8]
ladies_list = c(#"Linn Svahn",
                "Therese Johaug",
                #"Jessie Diggins",
                "Marit Bjørgen"
                #"Anamarija Lampic"
                )
lady = ladies[ladies_list, ]
lady = rbind(rep(2000,7), rep(1200, 7), lady)
radarchart(lady)

radarchart(ladies_all_k, axistype=1,
           
           )


men = read_excel("/Users/syverjohansen/ski/elo/python/ski/radar/max_man_values.xlsx", sheet="Sheet1", col_names=TRUE, na="NA")
men = data.frame(men[,2:9])
which(men$Name=="Gustaf Berglund")
men$Name[2312] = "Alexander Schwarz2"
men$Name[2363] = "Alexander Schwarz3"
men$Name[1186] = "David Rees2"
men$Name[604] = "Gunnar Eriksson2"
men$Name[2034] = "Hannu Manninen2"
men$Name[1893] = "Peter Klofutar2"
men$Name[4032] = "Gustaf Berglund2"
rownames(men) = men[,1]
men = men[,2:8]




men_list = c("Bjørn Dæhlie"
            # "Thomas Alsgaard"
             #"Petter Northug",
             #"Johannes Høsflot Klæbo",
             #"Emil Iversen",
             #"Alexander Bolshunov"
             #"Sjur Røthe",
             #"Hans Christer Holund",
             #"Federico Pellegrino"
             )
man = men[men_list, ]
man = rbind(rep(2000,7), rep(1300, 7), man)
radarchart(man)

data <- as.data.frame(matrix( sample( 2:20 , 10 , replace=T) , ncol=10))
colnames(data) <- c("math" , "english" , "biology" , "music" , "R-coding", "data-viz" , "french" , "physic", "statistic", "sport" )

# To use the fmsb package, I have to add 2 lines to the dataframe: the max and min of each topic to show on the plot!
data <- rbind(rep(20,10) , rep(0,10) , data)
radarchart(data)

data <- as.data.frame(matrix( sample( 0:20 , 15 , replace=F) , ncol=5))
colnames(data) <- c("math" , "english" , "biology" , "music" , "R-coding" )
rownames(data) <- paste("mister" , letters[1:3] , sep="-")

# To use the fmsb package, I have to add 2 lines to the dataframe: the max and min of each variable to show on the plot!
data <- rbind(rep(20,5) , rep(0,5) , data)

# plot with default options:
radarchart(data)
