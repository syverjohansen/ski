library(readxl)
varmen <- read_excel("~/ski/elo/excel365/all.xlsx", 
                      sheet = "Men", col_names = FALSE, na = "NA")
varladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                         sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
varmendf <- data.frame(varmen)
names(varmendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
varmendf$Seasons = NA
varmendf[varmendf$Name=="Oddmund Jensen", ]
varmendf[2073,9] <- "Oddmund Jensen2"
#print(unique(varmendf$Discipline))
#malevardistances <-c("Sprint", "Stage") 
#malevardistances <- c("Sprint")
#malevardiscipline <- c("C", "N/A")
#malevardiscipline <- c("F", "N/A")
#print(unique(timmendf$Date))
malevardates <- c("20200500", "20201209")
#varmendf <- varmendf[as.character(varmendf$Distance) %in% malevardistances, ]
#varmendf <- varmendf[as.character(varmendf$Discipline) %in% malevardiscipline,]
varmendf <- subset(varmendf, Date>malevardates[1] & Date<malevardates[2])
row.names(varmendf) <- 1:length(varmendf[,1])



varladiesdf <- data.frame(varladies)
names(varladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
varladiesdf$Seasons = NA
varladiesdf[4721, 9] <- "Tatjana Kuznetsova2"
#print(unique(varladiesdf$Discipline))
#ladyvardistances <- c("Sprint", "Stage")
ladyvardistances <- c("Sprint")
#ladyvardiscipline <- c("C", "NA", "N/A")
ladyvardiscipline <- c("F")
varladiesdf <- varladiesdf[as.character(varladiesdf$Distance) %in% ladyvardistances, ]
varladiesdf <- varladiesdf[as.character(varladiesdf$Discipline) %in% ladyvardiscipline,]
ladiesvardates <- c("20190500", "20200500")
varladiesdf <- subset(varladiesdf, Date>malevardates[1] & Date<malevardates[2])
row.names(varladiesdf) <- 1:length(varladiesdf[,1])


for(a in 1:length(varmendf$Date)){
  if((as.double(substr(varmendf$Date[a], 5, 8)) > 1000) && as.double(substr(varmendf$Date[a],5,8)!=9999)){
    varmendf$Seasons[a] = as.character(as.double(substr(varmendf$Date[a], 1, 4))+1)
  }
  else{
    varmendf$Seasons[a] = as.character(as.double(substr(varmendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(varladiesdf$Date)){
  if((as.double(substr(varladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(varladiesdf$Date[a],5,8)!=9999)){
    varladiesdf$Seasons[a] = as.character(as.double(substr(varladiesdf$Date[a], 1, 4))+1)
  }
  else{
    varladiesdf$Seasons[a] = as.character(as.double(substr(varladiesdf$Date[a], 1, 4)))
  }
}

varmaleraces = list()
varladyraces = list()

for(a in 1:length(varmendf$Seasons)){
  if(varmendf$Seasons[a] %in% names(varmaleraces)==FALSE){
    varmaleraces[[varmendf$Seasons[a]]] <- list()
  }
  
  if(varmendf$Date[a] %in% names(varmaleraces[[varmendf$Seasons[a]]])==FALSE){
    varmaleraces[[varmendf$Seasons[a]]][[varmendf$Date[a]]] <- list()
  }
  if(varmendf$Distance[a] %in% names(varmaleraces[[varmendf$Seasons[a]]][[varmendf$Date[a]]])==FALSE){
    varmaleraces[[varmendf$Seasons[a]]][[varmendf$Date[a]]][[varmendf$Distance[a]]]<-list()
  }
  if(varmendf$Name[a] %in% names(varmaleraces[[varmendf$Seasons[a]]][[varmendf$Date[a]]][[varmendf$Distance[a]]])==FALSE){
    varmaleraces[[varmendf$Seasons[a]]][[varmendf$Date[a]]][[varmendf$Distance[a]]][[varmendf$Name[a]]]<-as.double(varmendf$Place[a])
  }
}

for(a in 1:length(varladiesdf$Seasons)){
  if(varladiesdf$Seasons[a] %in% names(varladyraces)==FALSE){
    varladyraces[[varladiesdf$Seasons[a]]] <- list()
  }
  
  if(varladiesdf$Date[a] %in% names(varladyraces[[varladiesdf$Seasons[a]]])==FALSE){
    varladyraces[[varladiesdf$Seasons[a]]][[varladiesdf$Date[a]]] <- list()
  }
  if(varladiesdf$Distance[a] %in% names(varladyraces[[varladiesdf$Seasons[a]]][[varladiesdf$Date[a]]])==FALSE){
    varladyraces[[varladiesdf$Seasons[a]]][[varladiesdf$Date[a]]][[varladiesdf$Distance[a]]]<-list()
  }
  if(varladiesdf$Name[a] %in% names(varladyraces[[varladiesdf$Seasons[a]]][[varladiesdf$Date[a]]][[varladiesdf$Distance[a]]])==FALSE){
    varladyraces[[varladiesdf$Seasons[a]]][[varladiesdf$Date[a]]][[varladiesdf$Distance[a]]][[varladiesdf$Name[a]]]<-as.double(varladiesdf$Place[a])
  }
}

savevarmaleraces<-varmaleraces
savevarladyraces <- varladyraces

#Initialize all skiers to have an Elo of 1300
varmaleelo = list()
varladyelo = list()
for(a in 1:length(varmendf$Name)){
  if(varmendf$Name[a] %in% names(varmaleelo) == FALSE){
    varmaleelo[[varmendf$Name[a]]] <- list()
    varmaleelo[[varmendf$Name[a]]][["0000"]]<-list()
    varmaleelo[[varmendf$Name[a]]][["0000"]][["00000000"]]<-list()
    varmaleelo[[varmendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(varladiesdf$Name)){
  if(varladiesdf$Name[a] %in% names(varladyelo) == FALSE){
    varladyelo[[varladiesdf$Name[a]]] <- list()
    varladyelo[[varladiesdf$Name[a]]][["0000"]]<-list()
    varladyelo[[varladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    varladyelo[[varladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savevarmaleelo<-varmaleelo
savevarladyelo <- varladyelo

varmaleelo<-savevarmaleelo

allelo <- c()
K=1
place_index = 1

for(z in 1:length(varmaleraces)){
  print(z)
  
  for(a in 1:length(varmaleraces[[z]])){
    
    
    for(b in 1:length(varmaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(varmaleraces[[z]][[a]][[b]])){
        lastp = varmaleelo[[names(varmaleraces[[z]][[a]][[b]][c])]][[length(varmaleelo[[names(varmaleraces[[z]][[a]][[b]][c])]])]][[length(varmaleelo[[names(varmaleraces[[z]][[a]][[b]][c])]][[length(varmaleelo[[names(varmaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(varmaleraces[[z]][[a]][[b]]), as.double(varmendf$Place[place_index:(place_index+length(names(varmaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(varmaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(varmaleraces[[z]][[a]][[b]][c])
        current_year <- names(varmaleraces[z])
        if(current_year %in% names(varmaleelo[[names(varmaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          varmaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(varmaleraces[[z]][a])
        if(current_date %in% names(varmaleelo[[names(varmaleraces[[z]][[a]][[b]][c])]][[names(varmaleraces[z])]])==FALSE){
          varmaleelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(varmaleraces[[z]][[a]][b])
        if(current_race %in% names(varmaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          varmaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(varmaleelo)){
    if(length(varmaleelo[[d]])>1){
      plastp = varmaleelo[[d]][[length(varmaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #varmaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      varmaleelo[[d]][[current_year]][[current_date]] <- list()
      varmaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)
varladyelo <- savevarladyelo
place_index = 1
allelo <- c()
for(z in 1:length(varladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(varladyraces[[z]])){
    
    
    for(b in 1:length(varladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(varladyraces[[z]][[a]][[b]])){
        lastp = varladyelo[[names(varladyraces[[z]][[a]][[b]][c])]][[length(varladyelo[[names(varladyraces[[z]][[a]][[b]][c])]])]][[length(varladyelo[[names(varladyraces[[z]][[a]][[b]][c])]][[length(varladyelo[[names(varladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(varladyraces[[z]][[a]][[b]]), as.double(varladiesdf$Place[place_index:(place_index+length(names(varladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(varladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(varladyraces[[z]][[a]][[b]][c])
        current_year <- names(varladyraces[z])
        if(current_year %in% names(varladyelo[[names(varladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          varladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(varladyraces[[z]][a])
        if(current_date %in% names(varladyelo[[names(varladyraces[[z]][[a]][[b]][c])]][[names(varladyraces[z])]])==FALSE){
          varladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(varladyraces[[z]][[a]][b])
        if(current_race %in% names(varladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          varladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(varladyelo)){
    if(length(varladyelo[[d]])>1){
      plastp = varladyelo[[d]][[length(varladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #varladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      varladyelo[[d]][[current_year]][[current_date]] <- list()
      varladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)


varmaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

varmalenames = c()
blseason = c()
varmaledate = c()
varmaledistance = c()
varmaleeloscore = c()
for (a in 1:length(varmaleelo)){
  tick=0
  for(b in 1:length(varmaleelo[[a]])){
    for(c in 1:length(varmaleelo[[a]][[b]])){
      for(d in 1:length(varmaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #varmalenames2 = append(varmalenames2,as.character(names(varmaleelo[a])))
      }
    }
  }
  varmalenames = append(varmalenames, rep(as.character(names(varmaleelo[a])), tick))
}


for (a in 1:length(varmaleelo)){
  for(b in 1:length(varmaleelo[[a]])){
    for(c in 1:length(varmaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(varmaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      varmaledate=append(varmaledate, rep(as.character(names(varmaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(varmaleelo)){
  for(b in 1:length(varmaleelo[[a]])){
    for(c in 1:length(varmaleelo[[a]][[b]])){
      for(d in 1:length(varmaleelo[[a]][[b]][[c]])){
        varmaledistance=append(varmaledistance, as.character(names(varmaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(varmaleelo)){
  for(b in 1:length(varmaleelo[[a]])){
    for(c in 1:length(varmaleelo[[a]][[b]])){
      for(d in 1:length(varmaleelo[[a]][[b]][[c]])){
        varmaleeloscore=append(varmaleeloscore, as.double(varmaleelo[[a]][[b]][[c]][[d]]))}}}}


varmaleelodf <- data.frame(varmalenames, varmaledate, varmaledistance, varmaleeloscore)



varladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

varladynames = c()
blseason = c()
varladydate = c()
varladydistance = c()
varladyeloscore = c()
for (a in 1:length(varladyelo)){
  tick=0
  for(b in 1:length(varladyelo[[a]])){
    for(c in 1:length(varladyelo[[a]][[b]])){
      for(d in 1:length(varladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #varladynames2 = append(varladynames2,as.character(names(varladyelo[a])))
      }
    }
  }
  varladynames = append(varladynames, rep(as.character(names(varladyelo[a])), tick))
}


for (a in 1:length(varladyelo)){
  for(b in 1:length(varladyelo[[a]])){
    for(c in 1:length(varladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(varladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      varladydate=append(varladydate, rep(as.character(names(varladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(varladyelo)){
  for(b in 1:length(varladyelo[[a]])){
    for(c in 1:length(varladyelo[[a]][[b]])){
      for(d in 1:length(varladyelo[[a]][[b]][[c]])){
        varladydistance=append(varladydistance, as.character(names(varladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(varladyelo)){
  for(b in 1:length(varladyelo[[a]])){
    for(c in 1:length(varladyelo[[a]][[b]])){
      for(d in 1:length(varladyelo[[a]][[b]][[c]])){
        varladyeloscore=append(varladyeloscore, as.double(varladyelo[[a]][[b]][[c]][[d]]))}}}}


varladyelodf <- data.frame(varladynames, varladydate, varladydistance, varladyeloscore)

savevarmalenames <- varmalenames
savevarmaledistance <- varmaledistance
savevarmaledate <- varmaledate
savevarmaleeloscore <- varmaleeloscore
savevarladynames <- varladynames
savevarladydistance <- varladydistance
savevarladydate <- varladydate
savevarladyeloscore <- varladyeloscore


varmalehighest <- varmaleelodf[order(-varmaleelodf$varmaleeloscore), ]
varmalehighest_ind <- varmalehighest[match(unique(varmalehighest$varmalenames), varmalehighest$varmalenames), ]
row.names(varmalehighest_ind) <- 1:length(varmalehighest_ind[,1])
varmalehighest_ind[1:25,]


varladyhighest <- varladyelodf[order(-varladyelodf$varladyeloscore), ]
varladyhighest_ind <-varladyhighest[match(unique(varladyhighest$varladynames), varladyhighest$varladynames), ]
row.names(varladyhighest_ind) <- 1:length(varladyhighest_ind[,1])
varladyhighest_ind[1:25, ]

varmalelast_race <- varmaleelodf[varmaleelodf$varmaledate=="20200500",]
varmalelast_race[order(-varmalelast_race$varmaleeloscore),]

library(ggplot2)

jhk = varmaleelodf[(varmaleelodf$varmalenames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = varmaleelodf[(varmaleelodf$varmalenames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = varmaleelodf[(varmaleelodf$varmalenames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = varmaleelodf[(varmaleelodf$varmalenames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = varmaleelodf[(varmaleelodf$varmalenames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = varmaleelodf[(varmaleelodf$varmalenames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(varmaleeloscore), colour=varmalenames)) +geom_point()


bd = varmaleelodf[(varmaleelodf$varmalenames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = varmaleelodf[(varmaleelodf$varmalenames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = varmaleelodf[(varmaleelodf$varmalenames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = varmaleelodf[(varmaleelodf$varmalenames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = varmaleelodf[(varmaleelodf$varmalenames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = varmaleelodf[(varmaleelodf$varmalenames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = varmaleelodf[(varmaleelodf$varmalenames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = varmaleelodf[(varmaleelodf$varmalenames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(varmaleeloscore), colour=varmalenames)) +geom_point()


varmaleelodf[which(varmaleelodf$varmalenames=="Kevin Brochman"),]

jd = varladyelodf[(varladyelodf$varladynames)=="Jessica Diggins", ]
kr = varladyelodf[(varladyelodf$varladynames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(varladyeloscore), colour=varladynames)) +geom_point()

tj = varladyelodf[(varladyelodf$varladynames)=="Therese Johaug", ]
mb = varladyelodf[(varladyelodf$varladynames)=="Marit Bjørgen", ]
io = varladyelodf[(varladyelodf$varladynames)=="Ingvild Flugstad Østberg", ]
hw = varladyelodf[(varladyelodf$varladynames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(varladyeloscore), colour=varladynames)) +geom_point()


varladyelodf[(varladyelodf$varladynames)=="Rosie Brennan", ]


varmalelast_race <- varmaleelodf[varmaleelodf$varmaledate=="20200500",]
varmalelast_race <- varmalelast_race[order(-varmalelast_race$varmaleeloscore),]
row.names(varmalelast_race) <- 1:length(varmalelast_race[,1])
varmalelast_race[1:25,]

varladylast_race <- varladyelodf[varladyelodf$varladydate=="20200500", ]
varladylast_race <- varladylast_race[order(-varladylast_race$varladyeloscore),]
row.names(varladylast_race) <- 1:length(varladylast_race[,1])
varladylast_race[1:25,]


actvarmaleelodfnames <- unique(varmaleelodf$varmalenames)
for(a in 1:length(actvarmaleelodfnames)){
  actvarmaleskier <- varmaleelodf[varmaleelodf$varmalenames==actvarmaleelodfnames[a], ]
  #print(actvarmaleskier)
  row.names(actvarmaleskier) <- 1:length(actvarmaleskier[,1])
  
  #Starting from the back, the last varmaledistance that is not zero.  Then that one plus one is 
  actvarmaleraces <- as.numeric(as.character(row.names(actvarmaleskier[which(actvarmaleskier$varmaledistance!="0"), ])))
  actvarmalelast <- actvarmaleskier[1:actvarmaleraces[length(actvarmaleraces)]+1, ]
  
  if(a==1){
    row.names(actvarmalelast) <- 1:length(actvarmalelast[,1])
    actvarmaleelodf<-actvarmalelast
  }
  else{
    #print(length(actvarmaleelodfnames)-a)
    row.names(actvarmalelast) <- (length(actvarmaleelodf[,1])+1):(length(actvarmaleelodf[,1])+length(actvarmalelast[,1]))
    actvarmaleelodf <- rbind(actvarmaleelodf, actvarmalelast)
  }
}


actvarladyelodfnames <- unique(varladyelodf$varladynames)
for(a in 1:length(actvarladyelodfnames)){
  actvarladyskier <- varladyelodf[varladyelodf$varladynames==actvarladyelodfnames[a], ]
  #print(actvarladyskier)
  row.names(actvarladyskier) <- 1:length(actvarladyskier[,1])
  
  #Starting from the back, the last varladydistance that is not zero.  Then that one plus one is 
  actvarladyraces <- as.numeric(as.character(row.names(actvarladyskier[which(actvarladyskier$varladydistance!="0"), ])))
  actvarladylast <- actvarladyskier[1:actvarladyraces[length(actvarladyraces)]+1, ]
  
  if(a==1){
    row.names(actvarladylast) <- 1:length(actvarladylast[,1])
    actvarladyelodf<-actvarladylast
  }
  else{
    #print(length(actvarladyelodfnames)-a)
    row.names(actvarladylast) <- (length(actvarladyelodf[,1])+1):(length(actvarladyelodf[,1])+length(actvarladylast[,1]))
    actvarladyelodf <- rbind(actvarladyelodf, actvarladylast)
  }
}
actvarmalenation <- c()
actvarmaleseason <- c()
for(a in unique(sort(actvarmaleelodf$varmaledate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actvarmaleseason <- append(actvarmaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(actvarmaleelodf[actvarmaleelodf$varmaledate==a, 1])))
  }
  else{
    actvarmaleseason <- append(actvarmaleseason, rep(as.numeric(substr(a, 1, 4)), length(actvarmaleelodf[actvarmaleelodf$varmaledate==a, 1])))#myear <- order(-myear$mvarmaleeloscore)
  }
}

for(a in unique(actvarmaleelodf$varmalenames)){
  #print(a)
  actvarmalenation <- append(actvarmalenation, rep(mendf[mendf$Name==a,]$Nationality[length(mendf[mendf$Name==a,]$Nationality)], 
                                                     length(actvarmaleelodf[actvarmaleelodf$varmalenames==a,1])))
}
actvarmaleelodf$varmaleseason <- NA
actvarmaleelodf[order(as.character(actvarmaleelodf$varmaledate)),]$varmaleseason <- actvarmaleseason
actvarmaleelodf$varmalenation <- actvarmalenation

actvarladynation <- c()
actvarladyseason <- c()
for(a in unique(sort(actvarladyelodf$varladydate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actvarladyseason <- append(actvarladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(actvarladyelodf[actvarladyelodf$varladydate==a, 1])))
  }
  else{
    actvarladyseason <- append(actvarladyseason, rep(as.numeric(substr(a, 1, 4)), length(actvarladyelodf[actvarladyelodf$varladydate==a, 1])))#myear <- order(-myear$mvarmaleeloscore)
  }
}

for(a in unique(actvarladyelodf$varladynames)){
  #print(a)
  actvarladynation <- append(actvarladynation, rep(varladiesdf[varladiesdf$Name==a,]$Nationality[length(varladiesdf[varladiesdf$Name==a,]$Nationality)], 
                                                     length(actvarladyelodf[actvarladyelodf$varladynames==a,1])))
}
actvarladyelodf$varladynation <- actvarladynation
#actvarladyelodf <- as.data.frame(actvarladyelodf)
actvarladyelodf$varladyseason <- NA
actvarladyelodf[order(actvarladyelodf$varladydate),]$varladyseason <- (actvarladyseason)


actvarmaleelodf[actvarmaleelodf$varmalenames=="Johannes Høsflot Klæbo", ]
actvarladyelodf[actvarladyelodf$varladynames=="Jessica Diggins", ]

actvarmalelast_race <- actvarmaleelodf[actvarmaleelodf$varmaledate=="20200500",]
actvarmalelast_race <- actvarmalelast_race[order(-actvarmalelast_race$varmaleeloscore),]
row.names(actvarmalelast_race) <- 1:length(actvarmalelast_race[,1])
#actvarmalelast_race
actvarmalelast_race[1:25,]
actvarmalelast_race[actvarmalelast_race$varmalenames=="Dario Cologna", ]

actvarladylast_race <- actvarladyelodf[actvarladyelodf$varladydate=="20200500",]
actvarladylast_race <- actvarladylast_race[order(-actvarladylast_race$varladyeloscore),]
row.names(actvarladylast_race) <- 1:length(actvarladylast_race[,1])
actvarladylast_race[1:25, ]
actvarladylast_race[actvarladylast_race$varladynames=="Jessica Diggins", ]


varmaleseasonstandings <- actvarmaleelodf[which(endsWith(as.character(actvarmaleelodf$varmaledate), "0500") ), ]
varmaleseasonstandings <- varmaleseasonstandings[order((as.character(varmaleseasonstandings$varmaledate))), ]
varmalerank <- c()

for(a in unique(varmaleseasonstandings$varmaledate)){
  varmaleseasonstandings[varmaleseasonstandings$varmaledate==a, ] <- varmaleseasonstandings[varmaleseasonstandings$varmaledate==a, ][order(-varmaleseasonstandings[varmaleseasonstandings$varmaledate==a, ]$varmaleeloscore), ]
  varmalerank <- append(varmalerank, 1:length(varmaleseasonstandings[varmaleseasonstandings$varmaledate==a, 1]))
}
varmaleseasonstandings$varmalerank <- varmalerank
row.names(varmaleseasonstandings)<-1:length(varmaleseasonstandings[,1])
mwrong_names <- c()
# for(a in unique(varmaleseasonstandings$varmalenames)){
#   #varmaleseasonstandings[varmaleseasonstandings$varmalenames==a,] <- mendf
#   temp_nation <- mendf[mendf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     mwrong_names <- append(mwrong_names, a)
#   }
#   #if(length(varmaleseasonstandings[varmaleseasonstandings$varmalenames==a,1])>15){
#   #  print(a)
#   #}
#   varmaleseason_diff <- mendf[mendf$Name==a,]
#   #varmaleseasonnation <- append(varmaleseasonnation, mendf[mendf$Name==a,])
# }
# mwrong_names <- mwrong_names[! mwrong_names %in% c("Mikhail Devjatiarov", "Vladimir Smirnov", "Alexandr Uschkalenko", "Alexei Prokourorov", 
#                                 "Igor Badamchin", "Aleksandr Golubev", "Urmas Välbe", "Andrej Kirillov", "German Karatschewskij",
#                                 "Viatscheslav Plaksunov", "Gennadiy Lasutin", "Igor Obukhov", "Elmo Kassin", "Alexander Karatschewskij")]


varmaleseasonstandings[varmaleseasonstandings$varmaledate=="20200500", ]
# View(varmaleseasonstandings[varmaleseasonstandings$varmalenames=="Gunde Svan", ])
# View(mendf[mendf$Name=="Gunde Svan", ])
# View(varmaleseasonstandings[varmaleseasonstandings$varmalenation=="Norway",])
# View(varmaleseasonstandings)

varladyseasonstandings <- actvarladyelodf[which(endsWith(as.character(actvarladyelodf$varladydate), "0500") ), ]
varladyseasonstandings <- varladyseasonstandings[order((as.character(varladyseasonstandings$varladydate))), ]
varladyrank <- c()
for(a in unique(varladyseasonstandings$varladydate)){
  varladyseasonstandings[varladyseasonstandings$varladydate==a, ] <- 
    varladyseasonstandings[varladyseasonstandings$varladydate==a, ][order(-varladyseasonstandings[varladyseasonstandings$varladydate==a, ]$varladyeloscore), ]
  #lyear <- order(-lyear$lvarmaleeloscore)
  varladyrank <- append(varladyrank, 1:length(varladyseasonstandings[varladyseasonstandings$varladydate==a, 1]))
}
varladyseasonstandings$varladyrank <- varladyrank
row.names(varladyseasonstandings)<-1:length(varladyseasonstandings[,1])


lwrong_names <- c()
# for(a in unique(varladyseasonstandings$varladynames)){
#   #varmaleseasonstandings[varmaleseasonstandings$varmalenames==a,] <- varladiesdf
#   temp_nation <- varladiesdf[varladiesdf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     lwrong_names <- append(lwrong_names, a)
#   }
#  # if(length(varladyseasonstandings[varladyseasonstandings$varladynames==a,1])>20){
#   #  print(a)
#  # }
#   varladyseason_diff <- varladiesdf[varladiesdf$Name==a,]
#   #varmaleseasonnation <- append(varmaleseasonnation, varladiesdf[varladiesdf$Name==a,])
# }


varladyseasonstandings[varladyseasonstandings$varladydate=="20200500", ]
varladyseasonstandings[varladyseasonstandings$varladynames=="Jessica Diggins", ]
View(varladyseasonstandings)
View(varmaleseasonstandings)

varmalelast_race <- varmaleelodf[varmaleelodf$varmaledate=="20201129",]
varmalelast_race <- varmalelast_race[varmalelast_race$varmaledistance=="Stage", ]
varmalelast_race <- varmalelast_race[order(-varmalelast_race$varmaleeloscore),]
row.names(varmalelast_race) <- 1:length(varmalelast_race[,1])
varmalelast_race[1:25,]

varladylast_race <- varladyelodf[varladyelodf$varladydate=="20201129", ]
varladylast_race <- varladylast_race[varladylast_race$varladydistance=="Stage", ]
varladylast_race <- varladylast_race[order(-varladylast_race$varladyeloscore),]
row.names(varladylast_race) <- 1:length(varladylast_race[,1])
varladylast_race[1:25,]

