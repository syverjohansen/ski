library(readxl)
library(ggplot2)
men <- read_excel("~/ski/elo/excel365/all.xlsx", 
                  sheet = "Men", col_names = FALSE, na = "NA")
ladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                     sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
mendf <- data.frame(men)
names(mendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
mendf$Seasons = NA
ladiesdf <- data.frame(ladies)
names(ladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
ladiesdf$Seasons = NA
mendf[2073,9] <- "Oddmund Jensen2"
mendf[mendf$Name=="David Rees", ][2:4,9] <- "David Rees2"
mendf[mendf$Name=="Gunnar Eriksson", ][3:5,9] <- "Gunnar Eriksson2"
mendf[mendf$Name=="Alexander Schwarz", ][c(3, 5:8),9] <- "Alexander Schwarz2"
mendf[mendf$Name=="Peter Klofutar", ][2,9] <- "Peter Klofutar2"


for(a in 1:length(mendf$Date)){
  if((as.double(substr(mendf$Date[a], 5, 8)) > 1000) && as.double(substr(mendf$Date[a],5,8)!=9999)){
    mendf$Seasons[a] = as.character(as.double(substr(mendf$Date[a], 1, 4))+1)
  }
  else{
    mendf$Seasons[a] = as.character(as.double(substr(mendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(ladiesdf$Date)){
  if((as.double(substr(ladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(ladiesdf$Date[a],5,8)!=9999)){
    ladiesdf$Seasons[a] = as.character(as.double(substr(ladiesdf$Date[a], 1, 4))+1)
  }
  else{
    ladiesdf$Seasons[a] = as.character(as.double(substr(ladiesdf$Date[a], 1, 4)))
  }
}

ladiesdf[24331, 9] <- "Tatjana Kuznetsova2"
ladiesdf[ladiesdf$Name=="Lilia Vasilieva", ][38:39,9] <- "Lilia Vasilieva2"
maleraces = list()
ladyraces = list()

for(a in 1:length(mendf$Seasons)){
  if(mendf$Seasons[a] %in% names(maleraces)==FALSE){
    maleraces[[mendf$Seasons[a]]] <- list()
  }
  
  if(mendf$Date[a] %in% names(maleraces[[mendf$Seasons[a]]])==FALSE){
    maleraces[[mendf$Seasons[a]]][[mendf$Date[a]]] <- list()
  }
  if(mendf$Distance[a] %in% names(maleraces[[mendf$Seasons[a]]][[mendf$Date[a]]])==FALSE){
    maleraces[[mendf$Seasons[a]]][[mendf$Date[a]]][[mendf$Distance[a]]]<-list()
  }
  if(mendf$Name[a] %in% names(maleraces[[mendf$Seasons[a]]][[mendf$Date[a]]][[mendf$Distance[a]]])==FALSE){
    maleraces[[mendf$Seasons[a]]][[mendf$Date[a]]][[mendf$Distance[a]]][[mendf$Name[a]]]<-as.double(mendf$Place[a])
  }
}

for(a in 1:length(ladiesdf$Seasons)){
  if(ladiesdf$Seasons[a] %in% names(ladyraces)==FALSE){
    ladyraces[[ladiesdf$Seasons[a]]] <- list()
  }
  
  if(ladiesdf$Date[a] %in% names(ladyraces[[ladiesdf$Seasons[a]]])==FALSE){
    ladyraces[[ladiesdf$Seasons[a]]][[ladiesdf$Date[a]]] <- list()
  }
  if(ladiesdf$Distance[a] %in% names(ladyraces[[ladiesdf$Seasons[a]]][[ladiesdf$Date[a]]])==FALSE){
    ladyraces[[ladiesdf$Seasons[a]]][[ladiesdf$Date[a]]][[ladiesdf$Distance[a]]]<-list()
  }
  if(ladiesdf$Name[a] %in% names(ladyraces[[ladiesdf$Seasons[a]]][[ladiesdf$Date[a]]][[ladiesdf$Distance[a]]])==FALSE){
    ladyraces[[ladiesdf$Seasons[a]]][[ladiesdf$Date[a]]][[ladiesdf$Distance[a]]][[ladiesdf$Name[a]]]<-as.double(ladiesdf$Place[a])
  }
}





savemaleraces<-maleraces
saveladyraces <- ladyraces

#Initialize all skiers to have an Elo of 1300
maleelo = list()
ladyelo = list()
for(a in 1:length(mendf$Name)){
  if(mendf$Name[a] %in% names(maleelo) == FALSE){
    maleelo[[mendf$Name[a]]] <- list()
    maleelo[[mendf$Name[a]]][["0000"]]<-list()
    maleelo[[mendf$Name[a]]][["0000"]][["00000000"]]<-list()
    maleelo[[mendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(ladiesdf$Name)){
  if(ladiesdf$Name[a] %in% names(ladyelo) == FALSE){
    ladyelo[[ladiesdf$Name[a]]] <- list()
    ladyelo[[ladiesdf$Name[a]]][["0000"]]<-list()
    ladyelo[[ladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    ladyelo[[ladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savemaleelo<-maleelo
saveladyelo <- ladyelo

#first attempt at an elo rating
maleelo<-savemaleelo

allelo <- c()
K=1
place_index = 1

for(z in 1:length(maleraces)){
  print(z)

for(a in 1:length(maleraces[[z]])){

  
  for(b in 1:length(maleraces[[z]][[a]])){
    pelo = c()
    elo = c()
    
    for(c in 1:length(maleraces[[z]][[a]][[b]])){
      lastp = maleelo[[names(maleraces[[z]][[a]][[b]][c])]][[length(maleelo[[names(maleraces[[z]][[a]][[b]][c])]])]][[length(maleelo[[names(maleraces[[z]][[a]][[b]][c])]][[length(maleelo[[names(maleraces[[z]][[a]][[b]][c])]])]])]]
      lastpp = lastp[[length(lastp)]]
      pelo = append(pelo, lastpp)                   
    }
    race_df <- data.frame(names(maleraces[[z]][[a]][[b]]), as.double(mendf$Place[place_index:(place_index+length(names(maleraces[[z]][[a]][[b]]))-1)]), pelo)
    place_index = place_index+ length(names(maleraces[[z]][[a]][[b]]))
    names(race_df) <-c("Name", "Place", "pelo")
  #  last = max(race_df$Place)
    #Now we calculate the Elo for each racer
    for (c in 1:length(race_df$pelo)){
      #Check to see if there is a list for season for the racer
      current_man <- names(maleraces[[z]][[a]][[b]][c])
      current_year <- names(maleraces[z])
      if(current_year %in% names(maleelo[[names(maleraces[[z]][[a]][[b]][c])]]) == FALSE){
        maleelo[[current_man]][[current_year]] <- list()
      }
      #Check to see if there is a list for date for the racer
      current_date <- names(maleraces[[z]][a])
      if(current_date %in% names(maleelo[[names(maleraces[[z]][[a]][[b]][c])]][[names(maleraces[z])]])==FALSE){
        maleelo[[current_man]][[current_year]][[current_date]]<-list()
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

      current_race <- names(maleraces[[z]][[a]][b])
      if(current_race %in% names(maleelo[[current_man]][[current_year]][[current_date]])==FALSE){
        maleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
      }
      elo = append(elo, r11)
      allelo=append(elo, r11)
    }
  }
}
  for(d in 1:length(maleelo)){
    if(length(maleelo[[d]])>1){
      plastp = maleelo[[d]][[length(maleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #maleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      maleelo[[d]][[current_year]][[current_date]] <- list()
      maleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
ladyelo <- saveladyelo
place_index = 1
allelo <- c()
for(z in 1:length(ladyraces)){

#for(z in 1:43){
  #print(z)
  
  for(a in 1:length(ladyraces[[z]])){
    
    
    for(b in 1:length(ladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(ladyraces[[z]][[a]][[b]])){
        lastp = ladyelo[[names(ladyraces[[z]][[a]][[b]][c])]][[length(ladyelo[[names(ladyraces[[z]][[a]][[b]][c])]])]][[length(ladyelo[[names(ladyraces[[z]][[a]][[b]][c])]][[length(ladyelo[[names(ladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(ladyraces[[z]][[a]][[b]]), as.double(ladiesdf$Place[place_index:(place_index+length(names(ladyraces[[z]][[a]][[b]]))-1)]), pelo)
    

      place_index = place_index+ length(names(ladyraces[[z]][[a]][[b]]))
    
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(ladyraces[[z]][[a]][[b]][c])
        current_year <- names(ladyraces[z])
        if(current_year %in% names(ladyelo[[names(ladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          ladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(ladyraces[[z]][a])
        if(current_date %in% names(ladyelo[[names(ladyraces[[z]][[a]][[b]][c])]][[names(ladyraces[z])]])==FALSE){
          ladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(ladyraces[[z]][[a]][b])
        if(current_race %in% names(ladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          ladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(ladyelo)){
    if(length(ladyelo[[d]])>1){
      plastp = ladyelo[[d]][[length(ladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #ladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      ladyelo[[d]][[current_year]][[current_date]] <- list()
      ladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}

#Now time to make a data frame

#Name, Season, Date, Distance, Elo
maleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

malenames = c()

maledate = c()
maledistance = c()
maleeloscore = c()
for (a in 1:length(maleelo)){
  tick=0
  for(b in 1:length(maleelo[[a]])){
    for(c in 1:length(maleelo[[a]][[b]])){
      for(d in 1:length(maleelo[[a]][[b]][[c]])){
        tick = tick+1
        #malenames2 = append(malenames2,as.character(names(maleelo[a])))
      }
    }
  }
  malenames = append(malenames, rep(as.character(names(maleelo[a])), tick))
}


for (a in 1:length(maleelo)){
  for(b in 1:length(maleelo[[a]])){
    for(c in 1:length(maleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(maleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      maledate=append(maledate, rep(as.character(names(maleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(maleelo)){
  for(b in 1:length(maleelo[[a]])){
    for(c in 1:length(maleelo[[a]][[b]])){
      for(d in 1:length(maleelo[[a]][[b]][[c]])){
        maledistance=append(maledistance, as.character(names(maleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(maleelo)){
  for(b in 1:length(maleelo[[a]])){
    for(c in 1:length(maleelo[[a]][[b]])){
      for(d in 1:length(maleelo[[a]][[b]][[c]])){
        maleeloscore=append(maleeloscore, as.double(maleelo[[a]][[b]][[c]][[d]]))}}}}


maleelodf <- data.frame(malenames, maledate, maledistance, maleeloscore)



ladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

ladynames = c()
bladyseason = c()
ladydate = c()
ladydistance = c()
ladyeloscore = c()
for (a in 1:length(ladyelo)){
  tick=0
  for(b in 1:length(ladyelo[[a]])){
    for(c in 1:length(ladyelo[[a]][[b]])){
      for(d in 1:length(ladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #ladynames2 = append(ladynames2,as.character(names(ladyelo[a])))
      }
    }
  }
  ladynames = append(ladynames, rep(as.character(names(ladyelo[a])), tick))
}


for (a in 1:length(ladyelo)){
  for(b in 1:length(ladyelo[[a]])){
    for(c in 1:length(ladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(ladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      ladydate=append(ladydate, rep(as.character(names(ladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(ladyelo)){
  for(b in 1:length(ladyelo[[a]])){
    for(c in 1:length(ladyelo[[a]][[b]])){
      for(d in 1:length(ladyelo[[a]][[b]][[c]])){
        ladydistance=append(ladydistance, as.character(names(ladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(ladyelo)){
  for(b in 1:length(ladyelo[[a]])){
    for(c in 1:length(ladyelo[[a]][[b]])){
      for(d in 1:length(ladyelo[[a]][[b]][[c]])){
        ladyeloscore=append(ladyeloscore, as.double(ladyelo[[a]][[b]][[c]][[d]]))}}}}


ladyelodf <- data.frame(ladynames, ladydate, ladydistance, ladyeloscore)

savemalenames <- malenames
savemaledistance <- maledistance
savemaledate <- maledate
savemaleeloscore <- maleeloscore
saveladynames <- ladynames
saveladydistance <- ladydistance
saveladydate <- ladydate
saveladyeloscore <- ladyeloscore



malehighest <- maleelodf[order(-maleelodf$maleeloscore), ]
malehighest_ind <- malehighest[match(unique(malehighest$malenames), malehighest$malenames), ]
row.names(malehighest_ind) <- 1:length(malehighest_ind[,1])
malehighest_ind[1:25,]
View(malehighest_ind)

ladyhighest <- ladyelodf[order(-ladyelodf$ladyeloscore), ]
ladyhighest_ind <-ladyhighest[match(unique(ladyhighest$ladynames), ladyhighest$ladynames), ]
row.names(ladyhighest_ind) <- 1:length(ladyhighest_ind[,1])
ladyhighest_ind[1:25, ]
View(ladyhighest_ind)

malelast_race <- maleelodf[maleelodf$maledate=="20201129",]
malelast_race[order(-malelast_race$maleeloscore),]

library(ggplot2)

jhk = maleelodf[(maleelodf$malenames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = maleelodf[(maleelodf$malenames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = maleelodf[(maleelodf$malenames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = maleelodf[(maleelodf$malenames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = maleelodf[(maleelodf$malenames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = maleelodf[(maleelodf$malenames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(maleeloscore), colour=malenames)) +geom_point()


bd = maleelodf[(maleelodf$malenames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = maleelodf[(maleelodf$malenames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = maleelodf[(maleelodf$malenames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = maleelodf[(maleelodf$malenames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = maleelodf[(maleelodf$malenames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = maleelodf[(maleelodf$malenames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = maleelodf[(maleelodf$malenames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = maleelodf[(maleelodf$malenames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(maleeloscore), colour=malenames)) +geom_point()


maleelodf[which(maleelodf$malenames=="Kevin Brochman"),]

jd = ladyelodf[(ladyelodf$ladynames)=="Jessica Diggins", ]
kr = ladyelodf[(ladyelodf$ladynames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(ladyeloscore), colour=ladynames)) +geom_point()

tj = ladyelodf[(ladyelodf$ladynames)=="Therese Johaug", ]
mb = ladyelodf[(ladyelodf$ladynames)=="Marit Bjørgen", ]
io = ladyelodf[(ladyelodf$ladynames)=="Ingvild Flugstad Østberg", ]
hw = ladyelodf[(ladyelodf$ladynames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(ladyeloscore), colour=ladynames)) +geom_point()


ladyelodf[(ladyelodf$ladynames)=="Rosie Brennan", ]


malelast_race <- maleelodf[maleelodf$maledate=="20201129",]
malelast_race <- malelast_race[malelast_race$maledistance=="Stage", ]
malelast_race <- malelast_race[order(-malelast_race$maleeloscore),]
row.names(malelast_race) <- 1:length(malelast_race[,1])
malelast_race[1:25,]

ladylast_race <- ladyelodf[ladyelodf$ladydate=="20201129", ]
ladylast_race <- ladylast_race[ladylast_race$ladydistance=="Stage", ]
ladylast_race <- ladylast_race[order(-ladylast_race$ladyeloscore),]
row.names(ladylast_race) <- 1:length(ladylast_race[,1])
ladylast_race[1:25,]


actmaleelodfnames <- unique(maleelodf$malenames)
for(a in 1:length(actmaleelodfnames)){
  actmaleskier <- maleelodf[maleelodf$malenames==actmaleelodfnames[a], ]
  #print(actmaleskier)
  row.names(actmaleskier) <- 1:length(actmaleskier[,1])
  
  #Starting from the back, the last maledistance that is not zero.  Then that one plus one is 
  actmaleraces <- as.numeric(as.character(row.names(actmaleskier[which(actmaleskier$maledistance!="0"), ])))
  actmalelast <- actmaleskier[1:actmaleraces[length(actmaleraces)]+1, ]
  
  if(a==1){
    row.names(actmalelast) <- 1:length(actmalelast[,1])
    actmaleelodf<-actmalelast
  }
  else{
    #print(length(actmaleelodfnames)-a)
    row.names(actmalelast) <- (length(actmaleelodf[,1])+1):(length(actmaleelodf[,1])+length(actmalelast[,1]))
    actmaleelodf <- rbind(actmaleelodf, actmalelast)
  }
}


actladyelodfnames <- unique(ladyelodf$ladynames)
for(a in 1:length(actladyelodfnames)){
  actladyskier <- ladyelodf[ladyelodf$ladynames==actladyelodfnames[a], ]
  #print(actladyskier)
  row.names(actladyskier) <- 1:length(actladyskier[,1])
  
  #Starting from the back, the last ladydistance that is not zero.  Then that one plus one is 
  actladyraces <- as.numeric(as.character(row.names(actladyskier[which(actladyskier$ladydistance!="0"), ])))
  actladylast <- actladyskier[1:actladyraces[length(actladyraces)]+1, ]
  
  if(a==1){
    row.names(actladylast) <- 1:length(actladylast[,1])
    actladyelodf<-actladylast
  }
  else{
    #print(length(actladyelodfnames)-a)
    row.names(actladylast) <- (length(actladyelodf[,1])+1):(length(actladyelodf[,1])+length(actladylast[,1]))
    actladyelodf <- rbind(actladyelodf, actladylast)
  }
}
actmalenation <- c()
actmaleseason <- c()
for(a in unique(sort(actmaleelodf$maledate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actmaleseason <- append(actmaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(actmaleelodf[actmaleelodf$maledate==a, 1])))
  }
  else{
  actmaleseason <- append(actmaleseason, rep(as.numeric(substr(a, 1, 4)), length(actmaleelodf[actmaleelodf$maledate==a, 1])))#myear <- order(-myear$mmaleeloscore)
}
}

for(a in unique(actmaleelodf$malenames)){
  #print(a)
  actmalenation <- append(actmalenation, rep(mendf[mendf$Name==a,]$Nationality[length(mendf[mendf$Name==a,]$Nationality)], 
                          length(actmaleelodf[actmaleelodf$malenames==a,1])))
}
actmaleelodf$maleseason <- NA
actmaleelodf[order(as.character(actmaleelodf$maledate)),]$maleseason <- actmaleseason

actmaleelodf$malenation <- actmalenation

actladynation <- c()
actladyseason <- c()
for(a in unique(sort(actladyelodf$ladydate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actladyseason <- append(actladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(actladyelodf[actladyelodf$ladydate==a, 1])))
  }
  else{
    actladyseason <- append(actladyseason, rep(as.numeric(substr(a, 1, 4)), length(actladyelodf[actladyelodf$ladydate==a, 1])))#myear <- order(-myear$mmaleeloscore)
  }
}

for(a in unique(actladyelodf$ladynames)){
  #print(a)
  actladynation <- append(actladynation, rep(ladiesdf[ladiesdf$Name==a,]$Nationality[length(ladiesdf[ladiesdf$Name==a,]$Nationality)], 
                                             length(actladyelodf[actladyelodf$ladynames==a,1])))
}
actladyelodf$ladynation <- actladynation
#actladyelodf <- as.data.frame(actladyelodf)
actladyelodf$ladyseason <- NA
actladyelodf[order(actladyelodf$ladydate),]$ladyseason <- (actladyseason)


actmaleelodf[actmaleelodf$malenames=="Johannes Høsflot Klæbo", ]
actladyelodf[actladyelodf$ladynames=="Jessica Diggins", ]

actmalelast_race <- actmaleelodf[actmaleelodf$maledate=="20201129",]
actmalelast_race <- actmalelast_race[order(-actmalelast_race$maleeloscore),]
row.names(actmalelast_race) <- 1:length(actmalelast_race[,1])
#actmalelast_race
actmalelast_race[1:25,]
actmalelast_race[actmalelast_race$malenames=="Dario Cologna", ]

actladylast_race <- actladyelodf[actladyelodf$ladydate=="20201129",]
actladylast_race <- actladylast_race[order(-actladylast_race$ladyeloscore),]
row.names(actladylast_race) <- 1:length(actladylast_race[,1])
actladylast_race[1:25, ]
actladylast_race[actladylast_race$ladynames=="Jessica Diggins", ]


maleseasonstandings <- actmaleelodf[which(endsWith(as.character(actmaleelodf$maledate), "0500") ), ]
maleseasonstandings <- maleseasonstandings[order((as.character(maleseasonstandings$maledate))), ]
malerank <- c()

for(a in unique(maleseasonstandings$maledate)){
  maleseasonstandings[maleseasonstandings$maledate==a, ] <- maleseasonstandings[maleseasonstandings$maledate==a, ][order(-maleseasonstandings[maleseasonstandings$maledate==a, ]$maleeloscore), ]
  malerank <- append(malerank, 1:length(maleseasonstandings[maleseasonstandings$maledate==a, 1]))
}
maleseasonstandings$malerank <- malerank
#maleseasonstandings$maleseason <- maleseasonstandingsseason
row.names(maleseasonstandings)<-1:length(maleseasonstandings[,1])
mwrong_names <- c()
# for(a in unique(maleseasonstandings$malenames)){
#   #maleseasonstandings[maleseasonstandings$malenames==a,] <- mendf
#   temp_nation <- mendf[mendf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     mwrong_names <- append(mwrong_names, a)
#   }
#   #if(length(maleseasonstandings[maleseasonstandings$malenames==a,1])>15){
#   #  print(a)
#   #}
#   maleseason_diff <- mendf[mendf$Name==a,]
#   #maleseasonnation <- append(maleseasonnation, mendf[mendf$Name==a,])
# }
# mwrong_names <- mwrong_names[! mwrong_names %in% c("Mikhail Devjatiarov", "Vladimir Smirnov", "Alexandr Uschkalenko", "Alexei Prokourorov", 
#                                 "Igor Badamchin", "Aleksandr Golubev", "Urmas Välbe", "Andrej Kirillov", "German Karatschewskij",
#                                 "Viatscheslav Plaksunov", "Gennadiy Lasutin", "Igor Obukhov", "Elmo Kassin", "Alexander Karatschewskij")]


#maleseasonstandings[maleseasonstandings$maledate=="20201129", ]
#View(maleseasonstandings[maleseasonstandings$malenames=="Gunde Svan", ])
#View(mendf[mendf$Name=="Gunde Svan", ])
#View(maleseasonstandings[maleseasonstandings$malenation=="Norway",])
#View(maleseasonstandings)

ladyseasonstandings <- actladyelodf[which(endsWith(as.character(actladyelodf$ladydate), "0500") ), ]
ladyseasonstandings <- ladyseasonstandings[order((as.character(ladyseasonstandings$ladydate))), ]
ladyrank <- c()
for(a in unique(ladyseasonstandings$ladydate)){
  ladyseasonstandings[ladyseasonstandings$ladydate==a, ] <- ladyseasonstandings[ladyseasonstandings$ladydate==a, ][order(-ladyseasonstandings[ladyseasonstandings$ladydate==a, ]$ladyeloscore), ]
  #lyear <- order(-lyear$lmaleeloscore)
  ladyrank <- append(ladyrank, 1:length(ladyseasonstandings[ladyseasonstandings$ladydate==a, 1]))
}
ladyseasonstandings$ladyrank <- ladyrank
row.names(ladyseasonstandings)<-1:length(ladyseasonstandings[,1])


lwrong_names <- c()
# for(a in unique(ladyseasonstandings$ladynames)){
#   #maleseasonstandings[maleseasonstandings$malenames==a,] <- ladiesdf
#   temp_nation <- ladiesdf[ladiesdf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     lwrong_names <- append(lwrong_names, a)
#   }
#  # if(length(ladyseasonstandings[ladyseasonstandings$ladynames==a,1])>20){
#   #  print(a)
#  # }
#   ladyseason_diff <- ladiesdf[ladiesdf$Name==a,]
#   #maleseasonnation <- append(maleseasonnation, ladiesdf[ladiesdf$Name==a,])
# }


ladyseasonstandings[ladyseasonstandings$ladydate=="20201129", ]
ladyseasonstandings[ladyseasonstandings$ladynames=="Jessica Diggins", ]
#View(ladiesdf[ladiesdf$Name=="Lilia Vasilieva", ])
#View(ladyseasonstandings[ladyseasonstandings$ladynation=="USA",])
#View(ladyseasonstandings)


View(ladyseasonstandings)

malecurrent_ranks = maleelodf[maleelodf$maledate=="20201129", ]
ladycurrent_ranks = ladyelodf[ladyelodf$ladydate=="20201129", ]

malecurrent_ranks = (malecurrent_ranks[order(-malecurrent_ranks$maleeloscore), ])
malecurrent_ranks$ranks = c(1:length(malecurrent_ranks[,1]))
ladycurrent_ranks = (ladycurrent_ranks[order(-ladycurrent_ranks$ladyeloscore), ])
ladycurrent_ranks$ranks = c(1:length(ladycurrent_ranks[,1]))
View(malecurrent_ranks)
View(ladycurrent_ranks)

maletop25 <- maleseasonstandings[maleseasonstandings$malenames %in% c(as.character(maleseasonstandings[(maleseasonstandings$maleseason==2020), ][1:20,]$malenames)), ]
for(a in unique(maletop25$malenames)){
  moldrank <- maletop25[maletop25$malenames==a,]
  moldrank <- moldrank[moldrank$maleseason==2018, ]$malerank
  mnewrank <- maletop25[maletop25$malenames==a,]
  mnewrank <- mnewrank[mnewrank$maleseason==2019, ]$malerank
  #print(c(a, moldrank-mnewrank))
  
}
maletop25[maletop25$maleseason==2020, 4]-maletop25[maletop25$mname==2019, 4]
ggplot(data=maletop25, aes(x=maleseason, y=0-malerank, group=malenames, col=malenames))+geom_point()+geom_line()+xlim(2019,2020)+ylim(-100,0)+geom_jitter()
maleusa <- (maleseasonstandings[maleseasonstandings$malenation=="USA",])
maleusa2020 <- maleusa[maleusa$malenames %in% c(as.character(maleusa[(maleusa$maleseason==2020), ]$malenames)), ]
ggplot(data=maleusa2020, aes(x=maleseason, y=0-malerank, group=malenames, col=malenames))+geom_point()+geom_line()+xlim(2015,2020)


ladyusa <- (ladyseasonstandings[ladyseasonstandings$ladynation=="USA",])
ladyusa2020 <- ladyusa[ladyusa$ladynames %in% c(as.character(ladyusa[(ladyusa$ladyseason==2020), ]$ladynames)), ]
ggplot(data=ladyusa2020, aes(x=ladyseason, y=0-ladyrank, group=ladynames, col=ladynames))+geom_point()+geom_line()+xlim(2015,2020)


lnor <- (ladyseasonstandings[ladyseasonstandings$ladynation=="Norway",])
lnor2020 <- lnor[lnor$ladynames %in% c(as.character(lnor[(lnor$ladyseason==2020), ]$ladynames)), ]
ggplot(data=lnor2020, aes(x=ladyseason, y=0-ladyrank, group=ladynames, col=ladynames))+geom_point()+geom_line()+xlim(2015,2020)


save_maleelodf <- maleelodf
save_ladyelodf <- ladyelodf


maleelo_write = maleelodf
ladyelo_write = ladyelodf

