library(readxl)
library(ggplot2)
bmen <- read_excel("~/ski/elo/biathlon/excel365/all.xlsx", 
                  sheet = "Men", col_names = FALSE, na = "NA")
bladies <- read_excel("~/ski/elo/biathlon/excel365/all.xlsx", 
                     sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
bmendf <- data.frame(bmen)
names(bmendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")

bladiesdf <- data.frame(bladies)
names(bladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")

bmendf[which(bmendf$Name=="Uros Velepec"), ]
bmendf[9757,9] = "Ricco Gross2"
bmendf[9698,9] = "Ricco Gross2"
bmendf[9365,9] = "Uros Velepec2"
bmendf[9360, 9] = "Petr Garabik2"
bmendf[9139,9] = "Alexandr Popov2"
bmendf[392,9] = "Sten Eriksson2"


bmendf$Seasons = NA
bladiesdf$Seasons = NA
for(a in 1:length(bmendf$Date)){
  if((as.double(substr(bmendf$Date[a], 5, 8)) > 1000) && as.double(substr(bmendf$Date[a],5,8)!=9999)){
    bmendf$Seasons[a] = as.character(as.double(substr(bmendf$Date[a], 1, 4))+1)
  }
  else{
    bmendf$Seasons[a] = as.character(as.double(substr(bmendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(bladiesdf$Date)){
  if((as.double(substr(bladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(bladiesdf$Date[a],5,8)!=9999)){
    bladiesdf$Seasons[a] = as.character(as.double(substr(bladiesdf$Date[a], 1, 4))+1)
  }
  else{
    bladiesdf$Seasons[a] = as.character(as.double(substr(bladiesdf$Date[a], 1, 4)))
  }
}

#bladiesdf[24331, 9] <- "Tatjana Kuznetsova2"
bmaleraces = list()
bladyraces = list()

for(a in 1:length(bmendf$Seasons)){
  if(bmendf$Seasons[a] %in% names(bmaleraces)==FALSE){
    bmaleraces[[bmendf$Seasons[a]]] <- list()
  }
  
  if(bmendf$Date[a] %in% names(bmaleraces[[bmendf$Seasons[a]]])==FALSE){
    bmaleraces[[bmendf$Seasons[a]]][[bmendf$Date[a]]] <- list()
  }
  if(bmendf$Distance[a] %in% names(bmaleraces[[bmendf$Seasons[a]]][[bmendf$Date[a]]])==FALSE){
    bmaleraces[[bmendf$Seasons[a]]][[bmendf$Date[a]]][[bmendf$Distance[a]]]<-list()
  }
  if(bmendf$Name[a] %in% names(bmaleraces[[bmendf$Seasons[a]]][[bmendf$Date[a]]][[bmendf$Distance[a]]])==FALSE){
    bmaleraces[[bmendf$Seasons[a]]][[bmendf$Date[a]]][[bmendf$Distance[a]]][[bmendf$Name[a]]]<-as.double(bmendf$Place[a])
  }
}

for(a in 1:length(bladiesdf$Seasons)){
  if(bladiesdf$Seasons[a] %in% names(bladyraces)==FALSE){
    bladyraces[[bladiesdf$Seasons[a]]] <- list()
  }
  
  if(bladiesdf$Date[a] %in% names(bladyraces[[bladiesdf$Seasons[a]]])==FALSE){
    bladyraces[[bladiesdf$Seasons[a]]][[bladiesdf$Date[a]]] <- list()
  }
  if(bladiesdf$Distance[a] %in% names(bladyraces[[bladiesdf$Seasons[a]]][[bladiesdf$Date[a]]])==FALSE){
    bladyraces[[bladiesdf$Seasons[a]]][[bladiesdf$Date[a]]][[bladiesdf$Distance[a]]]<-list()
  }
  if(bladiesdf$Name[a] %in% names(bladyraces[[bladiesdf$Seasons[a]]][[bladiesdf$Date[a]]][[bladiesdf$Distance[a]]])==FALSE){
    bladyraces[[bladiesdf$Seasons[a]]][[bladiesdf$Date[a]]][[bladiesdf$Distance[a]]][[bladiesdf$Name[a]]]<-as.double(bladiesdf$Place[a])
  }
}





savebmaleraces<-bmaleraces
savebladyraces <- bladyraces

#Initialize all skiers to have an Elo of 1300
bmalebelo = list()
bladybelo = list()
for(a in 1:length(bmendf$Name)){
  if(bmendf$Name[a] %in% names(bmalebelo) == FALSE){
    bmalebelo[[bmendf$Name[a]]] <- list()
    bmalebelo[[bmendf$Name[a]]][["0000"]]<-list()
    bmalebelo[[bmendf$Name[a]]][["0000"]][["00000000"]]<-list()
    bmalebelo[[bmendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(bladiesdf$Name)){
  if(bladiesdf$Name[a] %in% names(bladybelo) == FALSE){
    bladybelo[[bladiesdf$Name[a]]] <- list()
    bladybelo[[bladiesdf$Name[a]]][["0000"]]<-list()
    bladybelo[[bladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    bladybelo[[bladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savebmalebelo<-bmalebelo
savebladybelo <- bladybelo

#first attempt at an belo rating
bmalebelo<-savebmalebelo

allbelo <- c()
K=1
place_index = 1

for(z in 1:length(bmaleraces)){
#for(z in 1:35){
 # print(z)
  
  for(a in 1:length(bmaleraces[[z]])){
    
    
    for(b in 1:length(bmaleraces[[z]][[a]])){
      pbelo = c()
      belo = c()
      
      for(c in 1:length(bmaleraces[[z]][[a]][[b]])){
        lastp = bmalebelo[[names(bmaleraces[[z]][[a]][[b]][c])]][[length(bmalebelo[[names(bmaleraces[[z]][[a]][[b]][c])]])]][[length(bmalebelo[[names(bmaleraces[[z]][[a]][[b]][c])]][[length(bmalebelo[[names(bmaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pbelo = append(pbelo, lastpp)                   
      }
      race_df <- data.frame(names(bmaleraces[[z]][[a]][[b]]), as.double(bmendf$Place[place_index:(place_index+length(names(bmaleraces[[z]][[a]][[b]]))-1)]), pbelo)
      place_index = place_index+ length(names(bmaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pbelo")
      
     
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pbelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(bmaleraces[[z]][[a]][[b]][c])
        current_year <- names(bmaleraces[z])
        if(current_year %in% names(bmalebelo[[names(bmaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          bmalebelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(bmaleraces[[z]][a])
        if(current_date %in% names(bmalebelo[[names(bmaleraces[[z]][[a]][[b]][c])]][[names(bmaleraces[z])]])==FALSE){
          bmalebelo[[current_man]][[current_year]][[current_date]]<-list()
        }
        
        #Check to see if there is a list for the current race
        
        
        # for(c in 21:21){
        wplaces = which(race_df$Place>race_df$Place[c])
        dplaces = which(race_df$Place==race_df$Place[c])
        lplaces = which(race_df$Place<race_df$Place[c])
        r1 = race_df$pbelo[c]
        R1 = 10^(r1/400)
        if(length(wplaces)>0){
          
          wR2 = sum(10^(race_df$pbelo[wplaces]/400))/length(wplaces)
          wE1 = R1/(R1+wR2)
          wS1 = length(wplaces)
        }
        else{
          wS1 = 0
          wE1 = 0
        }
        if(length(dplaces>1)){
          dR2 = sum(10^(race_df$pbelo[dplaces]/400))/length(dplaces)
          dE1 = R1/(R1+dR2)
          dS1 = length(dplaces)-1
        }
        else{
          dE1 = 0
          dS1 = 0
        }
        if(length(lplaces>0)){
          lR2 = sum(10^(race_df$pbelo[lplaces]/400))/length(lplaces)
          lE1 = R1/(R1+lR2)
          lS1 = length(lplaces)
        }
        else{
          lE1 = 0
          lS1 = 0
        }
        
        r11 = r1+wS1*K*(1-wE1)+dS1*K*(.5-dE1)+lS1*K*(0-lE1)
        # print(r11)
        
        current_race <- names(bmaleraces[[z]][[a]][b])
        if(current_race %in% names(bmalebelo[[current_man]][[current_year]][[current_date]])==FALSE){
          bmalebelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        belo = append(belo, r11)
        allbelo=append(belo, r11)
      }
    }
  }
  print(current_year)
  for(d in 1:length(bmalebelo)){
    if(length(bmalebelo[[d]])>1){
      plastp = bmalebelo[[d]][[length(bmalebelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #bmalebelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      bmalebelo[[d]][[current_year]][[current_date]] <- list()
      bmalebelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
bladybelo <- savebladybelo
place_index = 1
allbelo <- c()
for(z in 1:length(bladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(bladyraces[[z]])){
    
    
    for(b in 1:length(bladyraces[[z]][[a]])){
      pbelo = c()
      belo = c()
      
      for(c in 1:length(bladyraces[[z]][[a]][[b]])){
        lastp = bladybelo[[names(bladyraces[[z]][[a]][[b]][c])]][[length(bladybelo[[names(bladyraces[[z]][[a]][[b]][c])]])]][[length(bladybelo[[names(bladyraces[[z]][[a]][[b]][c])]][[length(bladybelo[[names(bladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pbelo = append(pbelo, lastpp)                   
      }
      
      race_df <- data.frame(names(bladyraces[[z]][[a]][[b]]), as.double(bladiesdf$Place[place_index:(place_index+length(names(bladyraces[[z]][[a]][[b]]))-1)]), pbelo)
      
      
      place_index = place_index+ length(names(bladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pbelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pbelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(bladyraces[[z]][[a]][[b]][c])
        current_year <- names(bladyraces[z])
        if(current_year %in% names(bladybelo[[names(bladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          bladybelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(bladyraces[[z]][a])
        if(current_date %in% names(bladybelo[[names(bladyraces[[z]][[a]][[b]][c])]][[names(bladyraces[z])]])==FALSE){
          bladybelo[[current_man]][[current_year]][[current_date]]<-list()
        }
        
        #Check to see if there is a list for the current race
        
        
        # for(c in 21:21){
        wplaces = which(race_df$Place>race_df$Place[c])
        dplaces = which(race_df$Place==race_df$Place[c])
        lplaces = which(race_df$Place<race_df$Place[c])
        r1 = race_df$pbelo[c]
        R1 = 10^(r1/400)
        if(length(wplaces)>0){
          
          wR2 = sum(10^(race_df$pbelo[wplaces]/400))/length(wplaces)
          wE1 = R1/(R1+wR2)
          wS1 = length(wplaces)
        }
        else{
          wS1 = 0
          wE1 = 0
        }
        if(length(dplaces>1)){
          dR2 = sum(10^(race_df$pbelo[dplaces]/400))/length(dplaces)
          dE1 = R1/(R1+dR2)
          dS1 = length(dplaces)-1
        }
        else{
          dE1 = 0
          dS1 = 0
        }
        if(length(lplaces>0)){
          lR2 = sum(10^(race_df$pbelo[lplaces]/400))/length(lplaces)
          lE1 = R1/(R1+lR2)
          lS1 = length(lplaces)
        }
        else{
          lE1 = 0
          lS1 = 0
        }
        
        r11 = r1+wS1*K*(1-wE1)+dS1*K*(.5-dE1)+lS1*K*(0-lE1)
        # print(r11)
        
        current_race <- names(bladyraces[[z]][[a]][b])
        if(current_race %in% names(bladybelo[[current_man]][[current_year]][[current_date]])==FALSE){
          bladybelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        belo = append(belo, r11)
        allbelo=append(belo, r11)
      }
    }
  }
  for(d in 1:length(bladybelo)){
    if(length(bladybelo[[d]])>1){
      plastp = bladybelo[[d]][[length(bladybelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #bladybelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      bladybelo[[d]][[current_year]][[current_date]] <- list()
      bladybelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}

#Now time to make a data frame

#Name, Season, Date, Distance, Elo
bmalebelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

bmnames = c()
blseason = c()
bmdate = c()
bmdistance = c()
bmbelo = c()
for (a in 1:length(bmalebelo)){
  tick=0
  for(b in 1:length(bmalebelo[[a]])){
    for(c in 1:length(bmalebelo[[a]][[b]])){
      for(d in 1:length(bmalebelo[[a]][[b]][[c]])){
        tick = tick+1
        #bmnames2 = append(bmnames2,as.character(names(bmalebelo[a])))
      }
    }
  }
  bmnames = append(bmnames, rep(as.character(names(bmalebelo[a])), tick))
}


for (a in 1:length(bmalebelo)){
  for(b in 1:length(bmalebelo[[a]])){
    for(c in 1:length(bmalebelo[[a]][[b]])){
      tick=0
      for(d in 1:length(bmalebelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      bmdate=append(bmdate, rep(as.character(names(bmalebelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(bmalebelo)){
  for(b in 1:length(bmalebelo[[a]])){
    for(c in 1:length(bmalebelo[[a]][[b]])){
      for(d in 1:length(bmalebelo[[a]][[b]][[c]])){
        bmdistance=append(bmdistance, as.character(names(bmalebelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(bmalebelo)){
  for(b in 1:length(bmalebelo[[a]])){
    for(c in 1:length(bmalebelo[[a]][[b]])){
      for(d in 1:length(bmalebelo[[a]][[b]][[c]])){
        bmbelo=append(bmbelo, as.double(bmalebelo[[a]][[b]][[c]][[d]]))}}}}


bmalebelodf <- data.frame(bmnames, bmdate, bmdistance, bmbelo)

savebmnames <- bmnames
savebmdate <- bmdate
savebmdistance <- bmdistance
savebmbelo <- bmbelo


bmalebelodf <- data.frame(bmnames, bmdate, bmdistance, bmbelo)



bladybelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

blnames = c()
blseason = c()
bldate = c()
bldistance = c()
blbelo = c()
for (a in 1:length(bladybelo)){
  tick=0
  for(b in 1:length(bladybelo[[a]])){
    for(c in 1:length(bladybelo[[a]][[b]])){
      for(d in 1:length(bladybelo[[a]][[b]][[c]])){
        tick = tick+1
        #blnames2 = append(blnames2,as.character(names(bladybelo[a])))
      }
    }
  }
  blnames = append(blnames, rep(as.character(names(bladybelo[a])), tick))
}


for (a in 1:length(bladybelo)){
  for(b in 1:length(bladybelo[[a]])){
    for(c in 1:length(bladybelo[[a]][[b]])){
      tick=0
      for(d in 1:length(bladybelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      bldate=append(bldate, rep(as.character(names(bladybelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(bladybelo)){
  for(b in 1:length(bladybelo[[a]])){
    for(c in 1:length(bladybelo[[a]][[b]])){
      for(d in 1:length(bladybelo[[a]][[b]][[c]])){
        bldistance=append(bldistance, as.character(names(bladybelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(bladybelo)){
  for(b in 1:length(bladybelo[[a]])){
    for(c in 1:length(bladybelo[[a]][[b]])){
      for(d in 1:length(bladybelo[[a]][[b]][[c]])){
        blbelo=append(blbelo, as.double(bladybelo[[a]][[b]][[c]][[d]]))}}}}


bladybelodf <- data.frame(blnames, bldate, bldistance, blbelo)




bmalehighest <-bmalebelodf[order(-bmalebelodf$bmbelo), ]
bmalehighest_ind <- bmalehighest[match(unique(bmalehighest$bmnames), bmalehighest$bmnames), ]
row.names(bmalehighest_ind) <- 1:length(bmalehighest_ind[,1])
bmalehighest_ind[1:25,]

bladyhighest <- bladybelodf[order(-bladybelodf$blbelo), ]
bladyhighest_ind <- bladyhighest[match(unique(bladyhighest$blnames), bladyhighest$blnames), ]
row.names(bladyhighest_ind) <- 1:length(bladyhighest_ind[,1])
bladyhighest_ind[1:25,]


jtb = bmalebelodf[which(bmalebelodf$bmnames=="Johannes Thingnes Bø"),]
jtb$race = 1:length(jtb[,1])
mf = bmalebelodf[(bmalebelodf$bmnames=="Martin Fourcade"),]
mf$race = 1:length(mf[,1])
oeb = bmalebelodf[(bmalebelodf$bmnames=="Ole Einar Bjørndalen"),]
oeb$race=1:length(oeb[,1])
ehs = bmalebelodf[(bmalebelodf$bmnames=="Emil Hegle Svendsen"),]
ehs$race=1:length(ehs[,1])
jmo <- rbind(jtb, mf, oeb, ehs)
ggplot(jmo, aes(race, as.double(bmbelo), colour=bmnames)) +geom_point()


jb = bmalebelodf[which(bmalebelodf$bmnames=="Jake Brown"),]
jb$race = 1:length(jb[,1])
ps = bmalebelodf[(bmalebelodf$bmnames=="Paul Schommer"),]
ps$race = 1:length(ps[,1])
ln = bmalebelodf[(bmalebelodf$bmnames=="Leif Nordgren"),]
ln$race=1:length(ln[,1])
sd = bmalebelodf[(bmalebelodf$bmnames=="Sean Doherty"),]
sd$race=1:length(sd[,1])
usa <- rbind(jb, ps, ln, sd)
ggplot(usa, aes(race, as.double(bmbelo), colour=bmnames)) +geom_point()

bmalebelodf[which(bmalebelodf$bmnames=="Jake Brown"),]
bmalebelodf[which(bmalebelodf$bmnames=="Patrick Weatherton"),]


mor = bladybelodf[which(bladybelodf$blnames=="Marte Olsbu Røiseland"),]
mor$race = 1:length(mor[,1])
mor2020 <- mor[which(as.numeric(as.character(mor$bldate))>20190500), ]
mor2020$race = 1:length(mor2020[,1])
doro = bladybelodf[which(bladybelodf$blnames=="Dorothea Wierer"),]
doro$race = 1:length(doro[,1])
doro2020 <- doro[which(as.numeric(as.character(doro$bldate))>20190500), ]
doro2020$race = 1:length(doro2020[,1])
hanao = bladybelodf[which(bladybelodf$blnames=="Hanna Öberg"),]
hanao$race = 1:length(hanao[,1])
hanao2020 <- hanao[which(as.numeric(as.character(hanao$bldate))>20190500), ]
hanao2020$race = 1:length(hanao2020[,1])
herm = bladybelodf[which(bladybelodf$blnames=="Denise Herrmann"),]
herm$race = 1:length(herm[,1])
herm2020 <- herm[which(as.numeric(as.character(herm$bldate))>20190500), ]
herm2020$race = 1:length(herm2020[,1])
preus = bladybelodf[which(bladybelodf$blnames=="Franziska Preuss"),]
preus$race = 1:length(preus[,1]) 
preus2020 <- preus[which(as.numeric(as.character(preus$bldate))>20190500), ]
preus2020$race = 1:length(preus2020[,1])
eck = bladybelodf[which(bladybelodf$blnames=="Tiril Eckhoff"),]
eck$race = 1:length(eck[,1]) 
eck2020 <- eck[which(as.numeric(as.character(eck$bldate))>20190500), ]
eck2020$race = 1:length(eck2020[,1])
btop_wom <- rbind(mor, doro, hanao, herm, preus, eck)
btop_wom20 <- rbind(mor2020, doro2020, hanao2020, herm2020, preus2020, eck2020)
ggplot(btop_wom20, aes(bldate, as.double(blbelo), colour=blnames)) +geom_point()






bmalebelodf[bmalebelodf$bmdate=="20200306",]



bmlast_race <- bmalebelodf[bmalebelodf$bmdate=="20201129",]
bmlast_race <- bmlast_race[order(-bmlast_race$bmbelo),]
row.names(bmlast_race) <- 1:length(bmlast_race[,1])
bmlast_race
bmlast_race[1:25,]
bmlast_race[bmlast_race$bmnames=="Jake Brown", ]

bllast_race <- bladybelodf[bladybelodf$bldate=="20201129",]
bllast_race <- bllast_race[order(-bllast_race$blbelo),]
row.names(bllast_race) <- 1:length(bllast_race[,1])
bllast_race[1:25, ]



actbmalebelodfnames <- unique(bmalebelodf$bmnames)
for(a in 1:length(actbmalebelodfnames)){
actbmskier <- bmalebelodf[bmalebelodf$bmnames==actbmalebelodfnames[a], ]
#print(actbmskier)
row.names(actbmskier) <- 1:length(actbmskier[,1])

#Starting from the back, the last bmdistance that is not zero.  Then that one plus one is 
actbmraces <- as.numeric(as.character(row.names(actbmskier[which(actbmskier$bmdistance!="0"), ])))
actbmlast <- actbmskier[1:actbmraces[length(actbmraces)]+1, ]

if(a==1){
  row.names(actbmlast) <- 1:length(actbmlast[,1])
  actbmalebelodf<-actbmlast
}
else{
  #print(length(actbmalebelodfnames)-a)
  row.names(actbmlast) <- (length(actbmalebelodf[,1])+1):(length(actbmalebelodf[,1])+length(actbmlast[,1]))
  actbmalebelodf <- rbind(actbmalebelodf, actbmlast)
}
}


actbladybelodfnames <- unique(bladybelodf$blnames)
for(a in 1:length(actbladybelodfnames)){
  actblskier <- bladybelodf[bladybelodf$blnames==actbladybelodfnames[a], ]
  #print(actblskier)
  row.names(actblskier) <- 1:length(actblskier[,1])
  
  #Starting from the back, the last bldistance that is not zero.  Then that one plus one is 
  actblraces <- as.numeric(as.character(row.names(actblskier[which(actblskier$bldistance!="0"), ])))
  actbllast <- actblskier[1:actblraces[length(actblraces)]+1, ]
  
  if(a==1){
    row.names(actbllast) <- 1:length(actbllast[,1])
    actbladybelodf<-actbllast
  }
  else{
    print(length(actbladybelodfnames)-a)
    row.names(actbllast) <- (length(actbladybelodf[,1])+1):(length(actbladybelodf[,1])+length(actbllast[,1]))
    actbladybelodf <- rbind(actbladybelodf, actbllast)
  }
}


actbmalenation <- c()
actbmaleseason <- c()
for(a in unique(sort(actbmalebelodf$bmdate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actbmaleseason <- append(actbmaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(actbmalebelodf[actbmalebelodf$bmdate==a, 1])))
  }
  else{
    actbmaleseason <- append(actbmaleseason, rep(as.numeric(substr(a, 1, 4)), length(actbmalebelodf[actbmalebelodf$bmdate==a, 1])))#myear <- order(-myear$mmbelo)
  }
}

for(a in unique(actbmalebelodf$bmnames)){
  #print(a)
  actbmalenation <- append(actbmalenation, rep(bmendf[bmendf$Name==a,]$Nationality[length(bmendf[bmendf$Name==a,]$Nationality)], 
                                               length(actbmalebelodf[actbmalebelodf$bmnames==a,1])))
}

actbmalebelodf$bmseason <- NA
actbmalebelodf[order(as.character(actbmalebelodf$bmdate)),]$bmseason <- actbmaleseason

actbmalebelodf$bmnation <- actbmalenation

actbladynation <- c()
actbladyseason <- c()
for(a in unique(sort(actbladybelodf$bldate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actbladyseason <- append(actbladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(actbladybelodf[actbladybelodf$bldate==a, 1])))
  }
  else{
    actbladyseason <- append(actbladyseason, rep(as.numeric(substr(a, 1, 4)), length(actbladybelodf[actbladybelodf$bldate==a, 1])))#myear <- order(-myear$mmbelo)
  }
}

for(a in unique(actbladybelodf$blnames)){
  #print(a)
  actbladynation <- append(actbladynation, rep(bladiesdf[bladiesdf$Name==a,]$Nationality[length(bladiesdf[bladiesdf$Name==a,]$Nationality)], 
                                               length(actbladybelodf[actbladybelodf$blnames==a,1])))
}
actbladybelodf$blnation <- actbladynation
#actbladybelodf <- as.data.frame(actbladybelodf)
actbladybelodf$blseason <- NA
actbladybelodf[order(actbladybelodf$bldate),]$blseason <- (actbladyseason)

actbmalebelodf[actbmalebelodf$bmnames=="Jake Brown", ]


actbmlast_race <- actbmalebelodf[actbmalebelodf$bmdate=="20201129",]
actbmlast_race <- actbmlast_race[order(-actbmlast_race$bmbelo),]
row.names(actbmlast_race) <- 1:length(actbmlast_race[,1])
#actbmlast_race
actbmlast_race[1:25,]
actbmlast_race[actbmlast_race$bmnames=="Jake Brown", ]

actbllast_race <- actbladybelodf[actbladybelodf$bldate=="20201129",]
actbllast_race <- actbllast_race[order(-actbllast_race$blbelo),]
row.names(actbllast_race) <- 1:length(actbllast_race[,1])
actbllast_race[1:25, ]


bmseasonstandings <- actbmalebelodf[which(endsWith(as.character(actbmalebelodf$bmdate), "0500") ), ]
bmseasonstandings <- bmseasonstandings[order((as.character(bmseasonstandings$bmdate))), ]
bmrank <- c()
for(a in unique(bmseasonstandings$bmdate)){
  bmseasonstandings[bmseasonstandings$bmdate==a, ] <- bmseasonstandings[bmseasonstandings$bmdate==a, ][order(-bmseasonstandings[bmseasonstandings$bmdate==a, ]$bmbelo), ]
  #bmyear <- order(-bmyear$bmmelo)
  bmrank <- append(bmrank, 1:length(bmseasonstandings[bmseasonstandings$bmdate==a, 1]))
}
bmseasonstandings$bmrank <- bmrank
row.names(bmseasonstandings)<-1:length(bmseasonstandings[,1])
bmseasonstandings[bmseasonstandings$bmdate=="20201129", ]
bmseasonstandings[bmseasonstandings$bmnames=="Martin Fourcade", ]
bmusa <- (bmseasonstandings[bmseasonstandings$bmnation=="USA", ])
View(bmseasonstandings)


blseasonstandings <- actbladybelodf[which(endsWith(as.character(actbladybelodf$bldate), "0500") ), ]
blseasonstandings <- blseasonstandings[order((as.character(blseasonstandings$bldate))), ]
blrank <- c()
for(a in unique(blseasonstandings$bldate)){
  blseasonstandings[blseasonstandings$bldate==a, ] <- blseasonstandings[blseasonstandings$bldate==a, ][order(-blseasonstandings[blseasonstandings$bldate==a, ]$blbelo), ]
  #blyear <- order(-blyear$blmelo)
  blrank <- append(blrank, 1:length(blseasonstandings[blseasonstandings$bldate==a, 1]))
}
blseasonstandings$blrank <- blrank
row.names(blseasonstandings)<-1:length(blseasonstandings[,1])
blseasonstandings[blseasonstandings$bldate=="20201129", ]
blseasonstandings[blseasonstandings$blnames=="Magdalena Forsberg", ]
blusa <- (blseasonstandings[blseasonstandings$blnation=="USA", ])
View(blseasonstandings)


bmalecurrent_ranks = bmalebelodf[bmalebelodf$bmdate=="20201129", ]
bladycurrent_ranks = bladybelodf[bladybelodf$bldate=="20201129", ]

bmalecurrent_ranks = (bmalecurrent_ranks[order(-bmalecurrent_ranks$bmbelo), ])
bmalecurrent_ranks$ranks = c(1:length(bmalecurrent_ranks[,1]))
bladycurrent_ranks = (bladycurrent_ranks[order(-bladycurrent_ranks$blbelo), ])
bladycurrent_ranks$ranks = c(1:length(bladycurrent_ranks[,1]))
View(bmalecurrent_ranks)
View(bladycurrent_ranks)


ggplot(data=bmusa, aes(x=bmseason, y=0-bmrank, group=bmnames, color=bmnames)) + geom_line() + geom_point()
ggplot(data=blusa, aes(x=blseason, y=0-blrank, group=blnames, color=blnames)) + geom_line() + geom_point()








 
save_bmalebelodf <- bmalebelodf
save_bladybelodf <- bladybelodf

bmalebelo_write <- bmalebelodf
bladybelo_write <- bladybelodf
