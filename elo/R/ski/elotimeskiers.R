library(readxl)
timemen <- read_excel("~/ski/elo/excel365/all.xlsx", 
                                  sheet = "Men", col_names = FALSE, na = "NA")
timeladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                                     sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
timemendf <- data.frame(timemen)
names(timemendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
timemendf$Seasons = NA
timemendf[timemendf$Name=="Oddmund Jensen", ]
timemendf[2073,9] <- "Oddmund Jensen2"
#print(unique(timemendf$Discipline))
#maletimedistances <-c("Sprint", "Stage") 
#maletimediscipline <- c("C", "N/A")
#print(unique(timmendf$Date))
maletimedates <- c("20190500", "20200500")
#timemendf <- timemendf[!as.character(timemendf$Distance) %in% maletimedistances, ]
#timemendf <- timemendf[as.character(timemendf$Discipline) %in% maletimediscipline,]
timemendf <- subset(timemendf, Date>maletimedates[1] & Date<maletimedates[2])
row.names(timemendf) <- 1:length(timemendf[,1])



timeladiesdf <- data.frame(timeladies)
names(timeladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
timeladiesdf$Seasons = NA
timeladiesdf[4721, 9] <- "Tatjana Kuznetsova2"
#print(unique(timeladiesdf$Discipline))
#ladytimedistances <- c("Sprint", "Stage")
#ladytimediscipline <- c("C", "NA", "N/A")

#timeladiesdf <- timeladiesdf[!as.character(timeladiesdf$Distance) %in% ladytimedistances, ]
#timeladiesdf <- timeladiesdf[as.character(timeladiesdf$Discipline) %in% ladytimediscipline,]
ladiestimedates <- c("20190500")
timeladiesdf <- subset(timeladiesdf, Date>maletimedates[1] & Date<maletimedates[2])
row.names(timeladiesdf) <- 1:length(timeladiesdf[,1])


for(a in 1:length(timemendf$Date)){
  if((as.double(substr(timemendf$Date[a], 5, 8)) > 1000) && as.double(substr(timemendf$Date[a],5,8)!=9999)){
    timemendf$Seasons[a] = as.character(as.double(substr(timemendf$Date[a], 1, 4))+1)
  }
  else{
    timemendf$Seasons[a] = as.character(as.double(substr(timemendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(timeladiesdf$Date)){
  if((as.double(substr(timeladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(timeladiesdf$Date[a],5,8)!=9999)){
    timeladiesdf$Seasons[a] = as.character(as.double(substr(timeladiesdf$Date[a], 1, 4))+1)
  }
  else{
    timeladiesdf$Seasons[a] = as.character(as.double(substr(timeladiesdf$Date[a], 1, 4)))
  }
}

timemaleraces = list()
timeladyraces = list()

for(a in 1:length(timemendf$Seasons)){
  if(timemendf$Seasons[a] %in% names(timemaleraces)==FALSE){
    timemaleraces[[timemendf$Seasons[a]]] <- list()
  }
  
  if(timemendf$Date[a] %in% names(timemaleraces[[timemendf$Seasons[a]]])==FALSE){
    timemaleraces[[timemendf$Seasons[a]]][[timemendf$Date[a]]] <- list()
  }
  if(timemendf$Distance[a] %in% names(timemaleraces[[timemendf$Seasons[a]]][[timemendf$Date[a]]])==FALSE){
    timemaleraces[[timemendf$Seasons[a]]][[timemendf$Date[a]]][[timemendf$Distance[a]]]<-list()
  }
  if(timemendf$Name[a] %in% names(timemaleraces[[timemendf$Seasons[a]]][[timemendf$Date[a]]][[timemendf$Distance[a]]])==FALSE){
    timemaleraces[[timemendf$Seasons[a]]][[timemendf$Date[a]]][[timemendf$Distance[a]]][[timemendf$Name[a]]]<-as.double(timemendf$Place[a])
  }
}

for(a in 1:length(timeladiesdf$Seasons)){
  if(timeladiesdf$Seasons[a] %in% names(timeladyraces)==FALSE){
    timeladyraces[[timeladiesdf$Seasons[a]]] <- list()
  }
  
  if(timeladiesdf$Date[a] %in% names(timeladyraces[[timeladiesdf$Seasons[a]]])==FALSE){
    timeladyraces[[timeladiesdf$Seasons[a]]][[timeladiesdf$Date[a]]] <- list()
  }
  if(timeladiesdf$Distance[a] %in% names(timeladyraces[[timeladiesdf$Seasons[a]]][[timeladiesdf$Date[a]]])==FALSE){
    timeladyraces[[timeladiesdf$Seasons[a]]][[timeladiesdf$Date[a]]][[timeladiesdf$Distance[a]]]<-list()
  }
  if(timeladiesdf$Name[a] %in% names(timeladyraces[[timeladiesdf$Seasons[a]]][[timeladiesdf$Date[a]]][[timeladiesdf$Distance[a]]])==FALSE){
    timeladyraces[[timeladiesdf$Seasons[a]]][[timeladiesdf$Date[a]]][[timeladiesdf$Distance[a]]][[timeladiesdf$Name[a]]]<-as.double(timeladiesdf$Place[a])
  }
}

savetimemaleraces<-timemaleraces
savetimeladyraces <- timeladyraces

#Initialize all skiers to have an Elo of 1300
timemaleelo = list()
timeladyelo = list()
for(a in 1:length(timemendf$Name)){
  if(timemendf$Name[a] %in% names(timemaleelo) == FALSE){
    timemaleelo[[timemendf$Name[a]]] <- list()
    timemaleelo[[timemendf$Name[a]]][["0000"]]<-list()
    timemaleelo[[timemendf$Name[a]]][["0000"]][["00000000"]]<-list()
    timemaleelo[[timemendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(timeladiesdf$Name)){
  if(timeladiesdf$Name[a] %in% names(timeladyelo) == FALSE){
    timeladyelo[[timeladiesdf$Name[a]]] <- list()
    timeladyelo[[timeladiesdf$Name[a]]][["0000"]]<-list()
    timeladyelo[[timeladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    timeladyelo[[timeladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savetimemaleelo<-timemaleelo
savetimeladyelo <- timeladyelo

timemaleelo<-savetimemaleelo

allelo <- c()
K=1
place_index = 1

for(z in 1:length(timemaleraces)){
  print(z)
  
  for(a in 1:length(timemaleraces[[z]])){
    
    
    for(b in 1:length(timemaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(timemaleraces[[z]][[a]][[b]])){
        lastp = timemaleelo[[names(timemaleraces[[z]][[a]][[b]][c])]][[length(timemaleelo[[names(timemaleraces[[z]][[a]][[b]][c])]])]][[length(timemaleelo[[names(timemaleraces[[z]][[a]][[b]][c])]][[length(timemaleelo[[names(timemaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(timemaleraces[[z]][[a]][[b]]), as.double(timemendf$Place[place_index:(place_index+length(names(timemaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(timemaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(timemaleraces[[z]][[a]][[b]][c])
        current_year <- names(timemaleraces[z])
        if(current_year %in% names(timemaleelo[[names(timemaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          timemaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(timemaleraces[[z]][a])
        if(current_date %in% names(timemaleelo[[names(timemaleraces[[z]][[a]][[b]][c])]][[names(timemaleraces[z])]])==FALSE){
          timemaleelo[[current_man]][[current_year]][[current_date]]<-list()
        }
        
        #Check to see if there is a list for the current race
        
        
        # for(c in 21:21){
        wplaces = which(race_df$Place>race_df$Place[c])
        dplaces = which(race_df$Place==race_df$Place[c])
        lplaces = which(race_df$Place<race_df$Place[c])
        r1 = race_df$pelo[c]
        R1 = 10^(r1/400)
        if(length(wplaces)>0){
          
          wR2 = sum(10^(race_df$pelo[wplaces]/400))/length(wplaces)
          wE1 = R1/(R1+wR2)
          wS1 = length(wplaces)
        }
        else{
          wS1 = 0
          wE1 = 0
        }
        if(length(dplaces>1)){
          dR2 = sum(10^(race_df$pelo[dplaces]/400))/length(dplaces)
          dE1 = R1/(R1+dR2)
          dS1 = length(dplaces)-1
        }
        else{
          dE1 = 0
          dS1 = 0
        }
        if(length(lplaces>0)){
          lR2 = sum(10^(race_df$pelo[lplaces]/400))/length(lplaces)
          lE1 = R1/(R1+lR2)
          lS1 = length(lplaces)
        }
        else{
          lE1 = 0
          lS1 = 0
        }
        
        r11 = r1+wS1*K*(1-wE1)+dS1*K*(.5-dE1)+lS1*K*(0-lE1)
        # print(r11)
        
        current_race <- names(timemaleraces[[z]][[a]][b])
        if(current_race %in% names(timemaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          timemaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(timemaleelo)){
    if(length(timemaleelo[[d]])>1){
      plastp = timemaleelo[[d]][[length(timemaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #timemaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      timemaleelo[[d]][[current_year]][[current_date]] <- list()
      timemaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)
timeladyelo <- savetimeladyelo
place_index = 1
allelo <- c()
for(z in 1:length(timeladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(timeladyraces[[z]])){
    
    
    for(b in 1:length(timeladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(timeladyraces[[z]][[a]][[b]])){
        lastp = timeladyelo[[names(timeladyraces[[z]][[a]][[b]][c])]][[length(timeladyelo[[names(timeladyraces[[z]][[a]][[b]][c])]])]][[length(timeladyelo[[names(timeladyraces[[z]][[a]][[b]][c])]][[length(timeladyelo[[names(timeladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(timeladyraces[[z]][[a]][[b]]), as.double(timeladiesdf$Place[place_index:(place_index+length(names(timeladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(timeladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(timeladyraces[[z]][[a]][[b]][c])
        current_year <- names(timeladyraces[z])
        if(current_year %in% names(timeladyelo[[names(timeladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          timeladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(timeladyraces[[z]][a])
        if(current_date %in% names(timeladyelo[[names(timeladyraces[[z]][[a]][[b]][c])]][[names(timeladyraces[z])]])==FALSE){
          timeladyelo[[current_man]][[current_year]][[current_date]]<-list()
        }
        
        #Check to see if there is a list for the current race
        
        
        # for(c in 21:21){
        wplaces = which(race_df$Place>race_df$Place[c])
        dplaces = which(race_df$Place==race_df$Place[c])
        lplaces = which(race_df$Place<race_df$Place[c])
        r1 = race_df$pelo[c]
        R1 = 10^(r1/400)
        if(length(wplaces)>0){
          
          wR2 = sum(10^(race_df$pelo[wplaces]/400))/length(wplaces)
          wE1 = R1/(R1+wR2)
          wS1 = length(wplaces)
        }
        else{
          wS1 = 0
          wE1 = 0
        }
        if(length(dplaces>1)){
          dR2 = sum(10^(race_df$pelo[dplaces]/400))/length(dplaces)
          dE1 = R1/(R1+dR2)
          dS1 = length(dplaces)-1
        }
        else{
          dE1 = 0
          dS1 = 0
        }
        if(length(lplaces>0)){
          lR2 = sum(10^(race_df$pelo[lplaces]/400))/length(lplaces)
          lE1 = R1/(R1+lR2)
          lS1 = length(lplaces)
        }
        else{
          lE1 = 0
          lS1 = 0
        }
        
        r11 = r1+wS1*K*(1-wE1)+dS1*K*(.5-dE1)+lS1*K*(0-lE1)
        # print(r11)
        
        current_race <- names(timeladyraces[[z]][[a]][b])
        if(current_race %in% names(timeladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          timeladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(timeladyelo)){
    if(length(timeladyelo[[d]])>1){
      plastp = timeladyelo[[d]][[length(timeladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #timeladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      timeladyelo[[d]][[current_year]][[current_date]] <- list()
      timeladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)


timemaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

timemalenames = c()
blseason = c()
timemaledate = c()
timemaledistance = c()
timemaleeloscore = c()
for (a in 1:length(timemaleelo)){
  tick=0
  for(b in 1:length(timemaleelo[[a]])){
    for(c in 1:length(timemaleelo[[a]][[b]])){
      for(d in 1:length(timemaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #timemalenames2 = append(timemalenames2,as.character(names(timemaleelo[a])))
      }
    }
  }
  timemalenames = append(timemalenames, rep(as.character(names(timemaleelo[a])), tick))
}


for (a in 1:length(timemaleelo)){
  for(b in 1:length(timemaleelo[[a]])){
    for(c in 1:length(timemaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(timemaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      timemaledate=append(timemaledate, rep(as.character(names(timemaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(timemaleelo)){
  for(b in 1:length(timemaleelo[[a]])){
    for(c in 1:length(timemaleelo[[a]][[b]])){
      for(d in 1:length(timemaleelo[[a]][[b]][[c]])){
        timemaledistance=append(timemaledistance, as.character(names(timemaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(timemaleelo)){
  for(b in 1:length(timemaleelo[[a]])){
    for(c in 1:length(timemaleelo[[a]][[b]])){
      for(d in 1:length(timemaleelo[[a]][[b]][[c]])){
        timemaleeloscore=append(timemaleeloscore, as.double(timemaleelo[[a]][[b]][[c]][[d]]))}}}}


timemaleelodf <- data.frame(timemalenames, timemaledate, timemaledistance, timemaleeloscore)



timeladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

timeladynames = c()
blseason = c()
timeladydate = c()
timeladydistance = c()
timeladyeloscore = c()
for (a in 1:length(timeladyelo)){
  tick=0
  for(b in 1:length(timeladyelo[[a]])){
    for(c in 1:length(timeladyelo[[a]][[b]])){
      for(d in 1:length(timeladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #timeladynames2 = append(timeladynames2,as.character(names(timeladyelo[a])))
      }
    }
  }
  timeladynames = append(timeladynames, rep(as.character(names(timeladyelo[a])), tick))
}


for (a in 1:length(timeladyelo)){
  for(b in 1:length(timeladyelo[[a]])){
    for(c in 1:length(timeladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(timeladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      timeladydate=append(timeladydate, rep(as.character(names(timeladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(timeladyelo)){
  for(b in 1:length(timeladyelo[[a]])){
    for(c in 1:length(timeladyelo[[a]][[b]])){
      for(d in 1:length(timeladyelo[[a]][[b]][[c]])){
        timeladydistance=append(timeladydistance, as.character(names(timeladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(timeladyelo)){
  for(b in 1:length(timeladyelo[[a]])){
    for(c in 1:length(timeladyelo[[a]][[b]])){
      for(d in 1:length(timeladyelo[[a]][[b]][[c]])){
        timeladyeloscore=append(timeladyeloscore, as.double(timeladyelo[[a]][[b]][[c]][[d]]))}}}}


timeladyelodf <- data.frame(timeladynames, timeladydate, timeladydistance, timeladyeloscore)

savetimemalenames <- timemalenames
savetimemaledistance <- timemaledistance
savetimemaledate <- timemaledate
savetimemaleeloscore <- timemaleeloscore
savetimeladynames <- timeladynames
savetimeladydistance <- timeladydistance
savetimeladydate <- timeladydate
savetimeladyeloscore <- timeladyeloscore


timemalehighest <- timemaleelodf[order(-timemaleelodf$timemaleeloscore), ]
timemalehighest_ind <- timemalehighest[match(unique(timemalehighest$timemalenames), timemalehighest$timemalenames), ]
row.names(timemalehighest_ind) <- 1:length(timemalehighest_ind[,1])
timemalehighest_ind[1:25,]


timeladyhighest <- timeladyelodf[order(-timeladyelodf$timeladyeloscore), ]
timeladyhighest_ind <-timeladyhighest[match(unique(timeladyhighest$timeladynames), timeladyhighest$timeladynames), ]
row.names(timeladyhighest_ind) <- 1:length(timeladyhighest_ind[,1])
timeladyhighest_ind[1:25, ]

timemalelast_race <- timemaleelodf[timemaleelodf$timemaledate=="20200500",]
timemalelast_race[order(-timemalelast_race$timemaleeloscore),]

library(ggplot2)

jhk = timemaleelodf[(timemaleelodf$timemalenames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = timemaleelodf[(timemaleelodf$timemalenames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = timemaleelodf[(timemaleelodf$timemalenames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = timemaleelodf[(timemaleelodf$timemalenames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = timemaleelodf[(timemaleelodf$timemalenames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = timemaleelodf[(timemaleelodf$timemalenames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(timemaleeloscore), colour=timemalenames)) +geom_point()


bd = timemaleelodf[(timemaleelodf$timemalenames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = timemaleelodf[(timemaleelodf$timemalenames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = timemaleelodf[(timemaleelodf$timemalenames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = timemaleelodf[(timemaleelodf$timemalenames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = timemaleelodf[(timemaleelodf$timemalenames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = timemaleelodf[(timemaleelodf$timemalenames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = timemaleelodf[(timemaleelodf$timemalenames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = timemaleelodf[(timemaleelodf$timemalenames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(timemaleeloscore), colour=timemalenames)) +geom_point()


timemaleelodf[which(timemaleelodf$timemalenames=="Kevin Brochman"),]

jd = timeladyelodf[(timeladyelodf$timeladynames)=="Jessica Diggins", ]
kr = timeladyelodf[(timeladyelodf$timeladynames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(timeladyeloscore), colour=timeladynames)) +geom_point()

tj = timeladyelodf[(timeladyelodf$timeladynames)=="Therese Johaug", ]
mb = timeladyelodf[(timeladyelodf$timeladynames)=="Marit Bjørgen", ]
io = timeladyelodf[(timeladyelodf$timeladynames)=="Ingvild Flugstad Østberg", ]
hw = timeladyelodf[(timeladyelodf$timeladynames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(timeladyeloscore), colour=timeladynames)) +geom_point()


timeladyelodf[(timeladyelodf$timeladynames)=="Rosie Brennan", ]


timemalelast_race <- timemaleelodf[timemaleelodf$timemaledate=="20200500",]
timemalelast_race <- timemalelast_race[order(-timemalelast_race$timemaleeloscore),]
row.names(timemalelast_race) <- 1:length(timemalelast_race[,1])
timemalelast_race[1:25,]

timeladylast_race <- timeladyelodf[timeladyelodf$timeladydate=="20200500", ]
timeladylast_race <- timeladylast_race[order(-timeladylast_race$timeladyeloscore),]
row.names(timeladylast_race) <- 1:length(timeladylast_race[,1])
timeladylast_race[1:25,]


acttimemaleelodfnames <- unique(timemaleelodf$timemalenames)
for(a in 1:length(acttimemaleelodfnames)){
  acttimemaleskier <- timemaleelodf[timemaleelodf$timemalenames==acttimemaleelodfnames[a], ]
  #print(acttimemaleskier)
  row.names(acttimemaleskier) <- 1:length(acttimemaleskier[,1])
  
  #Starting from the back, the last timemaledistance that is not zero.  Then that one plus one is 
  acttimemaleraces <- as.numeric(as.character(row.names(acttimemaleskier[which(acttimemaleskier$timemaledistance!="0"), ])))
  acttimemalelast <- acttimemaleskier[1:acttimemaleraces[length(acttimemaleraces)]+1, ]
  
  if(a==1){
    row.names(acttimemalelast) <- 1:length(acttimemalelast[,1])
    acttimemaleelodf<-acttimemalelast
  }
  else{
    #print(length(acttimemaleelodfnames)-a)
    row.names(acttimemalelast) <- (length(acttimemaleelodf[,1])+1):(length(acttimemaleelodf[,1])+length(acttimemalelast[,1]))
    acttimemaleelodf <- rbind(acttimemaleelodf, acttimemalelast)
  }
}


acttimeladyelodfnames <- unique(timeladyelodf$timeladynames)
for(a in 1:length(acttimeladyelodfnames)){
  acttimeladyskier <- timeladyelodf[timeladyelodf$timeladynames==acttimeladyelodfnames[a], ]
  #print(acttimeladyskier)
  row.names(acttimeladyskier) <- 1:length(acttimeladyskier[,1])
  
  #Starting from the back, the last timeladydistance that is not zero.  Then that one plus one is 
  acttimeladyraces <- as.numeric(as.character(row.names(acttimeladyskier[which(acttimeladyskier$timeladydistance!="0"), ])))
  acttimeladylast <- acttimeladyskier[1:acttimeladyraces[length(acttimeladyraces)]+1, ]
  
  if(a==1){
    row.names(acttimeladylast) <- 1:length(acttimeladylast[,1])
    acttimeladyelodf<-acttimeladylast
  }
  else{
    #print(length(acttimeladyelodfnames)-a)
    row.names(acttimeladylast) <- (length(acttimeladyelodf[,1])+1):(length(acttimeladyelodf[,1])+length(acttimeladylast[,1]))
    acttimeladyelodf <- rbind(acttimeladyelodf, acttimeladylast)
  }
}
acttimemalenation <- c()
acttimemaleseason <- c()
for(a in unique(sort(acttimemaleelodf$timemaledate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    acttimemaleseason <- append(acttimemaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(acttimemaleelodf[acttimemaleelodf$timemaledate==a, 1])))
  }
  else{
    acttimemaleseason <- append(acttimemaleseason, rep(as.numeric(substr(a, 1, 4)), length(acttimemaleelodf[acttimemaleelodf$timemaledate==a, 1])))#myear <- order(-myear$mtimemaleeloscore)
  }
}

for(a in unique(acttimemaleelodf$timemalenames)){
  #print(a)
  acttimemalenation <- append(acttimemalenation, rep(mendf[mendf$Name==a,]$Nationality[length(mendf[mendf$Name==a,]$Nationality)], 
                                                                             length(acttimemaleelodf[acttimemaleelodf$timemalenames==a,1])))
}
acttimemaleelodf$timemaleseason <- NA
acttimemaleelodf[order(as.character(acttimemaleelodf$timemaledate)),]$timemaleseason <- acttimemaleseason
acttimemaleelodf$timemalenation <- acttimemalenation

acttimeladynation <- c()
acttimeladyseason <- c()
for(a in unique(sort(acttimeladyelodf$timeladydate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    acttimeladyseason <- append(acttimeladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(acttimeladyelodf[acttimeladyelodf$timeladydate==a, 1])))
  }
  else{
    acttimeladyseason <- append(acttimeladyseason, rep(as.numeric(substr(a, 1, 4)), length(acttimeladyelodf[acttimeladyelodf$timeladydate==a, 1])))#myear <- order(-myear$mtimemaleeloscore)
  }
}

for(a in unique(acttimeladyelodf$timeladynames)){
  #print(a)
  acttimeladynation <- append(acttimeladynation, rep(timeladiesdf[timeladiesdf$Name==a,]$Nationality[length(timeladiesdf[timeladiesdf$Name==a,]$Nationality)], 
                                                                             length(acttimeladyelodf[acttimeladyelodf$timeladynames==a,1])))
}
acttimeladyelodf$timeladynation <- acttimeladynation
#acttimeladyelodf <- as.data.frame(acttimeladyelodf)
acttimeladyelodf$timeladyseason <- NA
acttimeladyelodf[order(acttimeladyelodf$timeladydate),]$timeladyseason <- (acttimeladyseason)


acttimemaleelodf[acttimemaleelodf$timemalenames=="Johannes Høsflot Klæbo", ]
acttimeladyelodf[acttimeladyelodf$timeladynames=="Jessica Diggins", ]

acttimemalelast_race <- acttimemaleelodf[acttimemaleelodf$timemaledate=="20200500",]
acttimemalelast_race <- acttimemalelast_race[order(-acttimemalelast_race$timemaleeloscore),]
row.names(acttimemalelast_race) <- 1:length(acttimemalelast_race[,1])
#acttimemalelast_race
acttimemalelast_race[1:25,]
acttimemalelast_race[acttimemalelast_race$timemalenames=="Dario Cologna", ]

acttimeladylast_race <- acttimeladyelodf[acttimeladyelodf$timeladydate=="20200500",]
acttimeladylast_race <- acttimeladylast_race[order(-acttimeladylast_race$timeladyeloscore),]
row.names(acttimeladylast_race) <- 1:length(acttimeladylast_race[,1])
acttimeladylast_race[1:25, ]
acttimeladylast_race[acttimeladylast_race$timeladynames=="Jessica Diggins", ]


timemaleseasonstandings <- acttimemaleelodf[which(endsWith(as.character(acttimemaleelodf$timemaledate), "0500") ), ]
timemaleseasonstandings <- timemaleseasonstandings[order((as.character(timemaleseasonstandings$timemaledate))), ]
timemalerank <- c()

for(a in unique(timemaleseasonstandings$timemaledate)){
  timemaleseasonstandings[timemaleseasonstandings$timemaledate==a, ] <- timemaleseasonstandings[timemaleseasonstandings$timemaledate==a, ][order(-timemaleseasonstandings[timemaleseasonstandings$timemaledate==a, ]$timemaleeloscore), ]
  timemalerank <- append(timemalerank, 1:length(timemaleseasonstandings[timemaleseasonstandings$timemaledate==a, 1]))
}
timemaleseasonstandings$timemalerank <- timemalerank
row.names(timemaleseasonstandings)<-1:length(timemaleseasonstandings[,1])
mwrong_names <- c()
# for(a in unique(timemaleseasonstandings$timemalenames)){
#   #timemaleseasonstandings[timemaleseasonstandings$timemalenames==a,] <- mendf
#   temp_nation <- mendf[mendf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     mwrong_names <- append(mwrong_names, a)
#   }
#   #if(length(timemaleseasonstandings[timemaleseasonstandings$timemalenames==a,1])>15){
#   #  print(a)
#   #}
#   timemaleseason_diff <- mendf[mendf$Name==a,]
#   #timemaleseasonnation <- append(timemaleseasonnation, mendf[mendf$Name==a,])
# }
# mwrong_names <- mwrong_names[! mwrong_names %in% c("Mikhail Devjatiarov", "Vladimir Smirnov", "Alexandr Uschkalenko", "Alexei Prokourorov", 
#                                 "Igor Badamchin", "Aleksandr Golubev", "Urmas Välbe", "Andrej Kirillov", "German Karatschewskij",
#                                 "Viatscheslav Plaksunov", "Gennadiy Lasutin", "Igor Obukhov", "Elmo Kassin", "Alexander Karatschewskij")]


timemaleseasonstandings[timemaleseasonstandings$timemaledate=="20200500", ]
# View(timemaleseasonstandings[timemaleseasonstandings$timemalenames=="Gunde Svan", ])
# View(mendf[mendf$Name=="Gunde Svan", ])
# View(timemaleseasonstandings[timemaleseasonstandings$timemalenation=="Norway",])
# View(timemaleseasonstandings)

timeladyseasonstandings <- acttimeladyelodf[which(endsWith(as.character(acttimeladyelodf$timeladydate), "0500") ), ]
timeladyseasonstandings <- timeladyseasonstandings[order((as.character(timeladyseasonstandings$timeladydate))), ]
timeladyrank <- c()
for(a in unique(timeladyseasonstandings$timeladydate)){
  timeladyseasonstandings[timeladyseasonstandings$timeladydate==a, ] <- 
    timeladyseasonstandings[timeladyseasonstandings$timeladydate==a, ][order(-timeladyseasonstandings[timeladyseasonstandings$timeladydate==a, ]$timeladyeloscore), ]
  #lyear <- order(-lyear$ltimemaleeloscore)
  timeladyrank <- append(timeladyrank, 1:length(timeladyseasonstandings[timeladyseasonstandings$timeladydate==a, 1]))
}
timeladyseasonstandings$timeladyrank <- timeladyrank
row.names(timeladyseasonstandings)<-1:length(timeladyseasonstandings[,1])


lwrong_names <- c()
# for(a in unique(timeladyseasonstandings$timeladynames)){
#   #timemaleseasonstandings[timemaleseasonstandings$timemalenames==a,] <- timeladiesdf
#   temp_nation <- timeladiesdf[timeladiesdf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     lwrong_names <- append(lwrong_names, a)
#   }
#  # if(length(timeladyseasonstandings[timeladyseasonstandings$timeladynames==a,1])>20){
#   #  print(a)
#  # }
#   timeladyseason_diff <- timeladiesdf[timeladiesdf$Name==a,]
#   #timemaleseasonnation <- append(timemaleseasonnation, timeladiesdf[timeladiesdf$Name==a,])
# }


timeladyseasonstandings[timeladyseasonstandings$timeladydate=="20200500", ]
timeladyseasonstandings[timeladyseasonstandings$timeladynames=="Jessica Diggins", ]
View(timeladyseasonstandings)
View(timemaleseasonstandings)

