library(readxl)
endurancemen <- read_excel("~/ski/elo/excel365/all.xlsx", 
                        sheet = "Men", col_names = FALSE, na = "NA")
enduranceladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                                 sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
endurancemendf <- data.frame(endurancemen)
names(endurancemendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
endurancemendf$Seasons = NA
#print(unique(endurancemendf$Distance))
maleendurancedistances <-c("Sprint", "Stage") 
endurancemendf <- endurancemendf[!as.character(endurancemendf$Distance) %in% maleendurancedistances, ]
row.names(endurancemendf) <- 1:length(endurancemendf[,1])
endurancemendf[endurancemendf$Name=="Oddmund Jensen", ]
endurancemendf[2073,9] <- "Oddmund Jensen2"

enduranceladiesdf <- data.frame(enduranceladies)
names(enduranceladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
enduranceladiesdf$Seasons = NA

ladyendurancedistances <- c("Sprint", "Stage")


enduranceladiesdf <- enduranceladiesdf[!as.character(enduranceladiesdf$Distance) %in% ladyendurancedistances, ]
row.names(enduranceladiesdf) <- 1:length(enduranceladiesdf[,1])
#enduranceladiesdf[4721, 9] <- "Tatjana Kuznetsova2"

for(a in 1:length(endurancemendf$Date)){
  if((as.double(substr(endurancemendf$Date[a], 5, 8)) > 1000) && as.double(substr(endurancemendf$Date[a],5,8)!=9999)){
    endurancemendf$Seasons[a] = as.character(as.double(substr(endurancemendf$Date[a], 1, 4))+1)
  }
  else{
    endurancemendf$Seasons[a] = as.character(as.double(substr(endurancemendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(enduranceladiesdf$Date)){
  if((as.double(substr(enduranceladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(enduranceladiesdf$Date[a],5,8)!=9999)){
    enduranceladiesdf$Seasons[a] = as.character(as.double(substr(enduranceladiesdf$Date[a], 1, 4))+1)
  }
  else{
    enduranceladiesdf$Seasons[a] = as.character(as.double(substr(enduranceladiesdf$Date[a], 1, 4)))
  }
}

endurancemaleraces = list()
enduranceladyraces = list()

for(a in 1:length(endurancemendf$Seasons)){
  if(endurancemendf$Seasons[a] %in% names(endurancemaleraces)==FALSE){
    endurancemaleraces[[endurancemendf$Seasons[a]]] <- list()
  }
  
  if(endurancemendf$Date[a] %in% names(endurancemaleraces[[endurancemendf$Seasons[a]]])==FALSE){
    endurancemaleraces[[endurancemendf$Seasons[a]]][[endurancemendf$Date[a]]] <- list()
  }
  if(endurancemendf$Distance[a] %in% names(endurancemaleraces[[endurancemendf$Seasons[a]]][[endurancemendf$Date[a]]])==FALSE){
    endurancemaleraces[[endurancemendf$Seasons[a]]][[endurancemendf$Date[a]]][[endurancemendf$Distance[a]]]<-list()
  }
  if(endurancemendf$Name[a] %in% names(endurancemaleraces[[endurancemendf$Seasons[a]]][[endurancemendf$Date[a]]][[endurancemendf$Distance[a]]])==FALSE){
    endurancemaleraces[[endurancemendf$Seasons[a]]][[endurancemendf$Date[a]]][[endurancemendf$Distance[a]]][[endurancemendf$Name[a]]]<-as.double(endurancemendf$Place[a])
  }
}

for(a in 1:length(enduranceladiesdf$Seasons)){
  if(enduranceladiesdf$Seasons[a] %in% names(enduranceladyraces)==FALSE){
    enduranceladyraces[[enduranceladiesdf$Seasons[a]]] <- list()
  }
  
  if(enduranceladiesdf$Date[a] %in% names(enduranceladyraces[[enduranceladiesdf$Seasons[a]]])==FALSE){
    enduranceladyraces[[enduranceladiesdf$Seasons[a]]][[enduranceladiesdf$Date[a]]] <- list()
  }
  if(enduranceladiesdf$Distance[a] %in% names(enduranceladyraces[[enduranceladiesdf$Seasons[a]]][[enduranceladiesdf$Date[a]]])==FALSE){
    enduranceladyraces[[enduranceladiesdf$Seasons[a]]][[enduranceladiesdf$Date[a]]][[enduranceladiesdf$Distance[a]]]<-list()
  }
  if(enduranceladiesdf$Name[a] %in% names(enduranceladyraces[[enduranceladiesdf$Seasons[a]]][[enduranceladiesdf$Date[a]]][[enduranceladiesdf$Distance[a]]])==FALSE){
    enduranceladyraces[[enduranceladiesdf$Seasons[a]]][[enduranceladiesdf$Date[a]]][[enduranceladiesdf$Distance[a]]][[enduranceladiesdf$Name[a]]]<-as.double(enduranceladiesdf$Place[a])
  }
}

saveendurancemaleraces<-endurancemaleraces
saveenduranceladyraces <- enduranceladyraces

#Initialize all skiers to have an Elo of 1300
endurancemaleelo = list()
enduranceladyelo = list()
for(a in 1:length(endurancemendf$Name)){
  if(endurancemendf$Name[a] %in% names(endurancemaleelo) == FALSE){
    endurancemaleelo[[endurancemendf$Name[a]]] <- list()
    endurancemaleelo[[endurancemendf$Name[a]]][["0000"]]<-list()
    endurancemaleelo[[endurancemendf$Name[a]]][["0000"]][["00000000"]]<-list()
    endurancemaleelo[[endurancemendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(enduranceladiesdf$Name)){
  if(enduranceladiesdf$Name[a] %in% names(enduranceladyelo) == FALSE){
    enduranceladyelo[[enduranceladiesdf$Name[a]]] <- list()
    enduranceladyelo[[enduranceladiesdf$Name[a]]][["0000"]]<-list()
    enduranceladyelo[[enduranceladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    enduranceladyelo[[enduranceladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
saveendurancemaleelo<-endurancemaleelo
saveenduranceladyelo <- enduranceladyelo

endurancemaleelo<-saveendurancemaleelo

allelo <- c()
K=1
place_index = 1

for(z in 1:length(endurancemaleraces)){
  print(z)
  
  for(a in 1:length(endurancemaleraces[[z]])){
    
    
    for(b in 1:length(endurancemaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(endurancemaleraces[[z]][[a]][[b]])){
        lastp = endurancemaleelo[[names(endurancemaleraces[[z]][[a]][[b]][c])]][[length(endurancemaleelo[[names(endurancemaleraces[[z]][[a]][[b]][c])]])]][[length(endurancemaleelo[[names(endurancemaleraces[[z]][[a]][[b]][c])]][[length(endurancemaleelo[[names(endurancemaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(endurancemaleraces[[z]][[a]][[b]]), as.double(endurancemendf$Place[place_index:(place_index+length(names(endurancemaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(endurancemaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(endurancemaleraces[[z]][[a]][[b]][c])
        current_year <- names(endurancemaleraces[z])
        if(current_year %in% names(endurancemaleelo[[names(endurancemaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          endurancemaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(endurancemaleraces[[z]][a])
        if(current_date %in% names(endurancemaleelo[[names(endurancemaleraces[[z]][[a]][[b]][c])]][[names(endurancemaleraces[z])]])==FALSE){
          endurancemaleelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(endurancemaleraces[[z]][[a]][b])
        if(current_race %in% names(endurancemaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          endurancemaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(endurancemaleelo)){
    if(length(endurancemaleelo[[d]])>1){
      plastp = endurancemaleelo[[d]][[length(endurancemaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #endurancemaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      endurancemaleelo[[d]][[current_year]][[current_date]] <- list()
      endurancemaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)
enduranceladyelo <- saveenduranceladyelo
place_index = 1
allelo <- c()
for(z in 1:length(enduranceladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(enduranceladyraces[[z]])){
    
    
    for(b in 1:length(enduranceladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(enduranceladyraces[[z]][[a]][[b]])){
        lastp = enduranceladyelo[[names(enduranceladyraces[[z]][[a]][[b]][c])]][[length(enduranceladyelo[[names(enduranceladyraces[[z]][[a]][[b]][c])]])]][[length(enduranceladyelo[[names(enduranceladyraces[[z]][[a]][[b]][c])]][[length(enduranceladyelo[[names(enduranceladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(enduranceladyraces[[z]][[a]][[b]]), as.double(enduranceladiesdf$Place[place_index:(place_index+length(names(enduranceladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(enduranceladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(enduranceladyraces[[z]][[a]][[b]][c])
        current_year <- names(enduranceladyraces[z])
        if(current_year %in% names(enduranceladyelo[[names(enduranceladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          enduranceladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(enduranceladyraces[[z]][a])
        if(current_date %in% names(enduranceladyelo[[names(enduranceladyraces[[z]][[a]][[b]][c])]][[names(enduranceladyraces[z])]])==FALSE){
          enduranceladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(enduranceladyraces[[z]][[a]][b])
        if(current_race %in% names(enduranceladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          enduranceladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(enduranceladyelo)){
    if(length(enduranceladyelo[[d]])>1){
      plastp = enduranceladyelo[[d]][[length(enduranceladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #enduranceladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      enduranceladyelo[[d]][[current_year]][[current_date]] <- list()
      enduranceladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)


endurancemaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

endurancemalenames = c()
blseason = c()
endurancemaledate = c()
endurancemaledistance = c()
endurancemaleeloscore = c()
for (a in 1:length(endurancemaleelo)){
  tick=0
  for(b in 1:length(endurancemaleelo[[a]])){
    for(c in 1:length(endurancemaleelo[[a]][[b]])){
      for(d in 1:length(endurancemaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #endurancemalenames2 = append(endurancemalenames2,as.character(names(endurancemaleelo[a])))
      }
    }
  }
  endurancemalenames = append(endurancemalenames, rep(as.character(names(endurancemaleelo[a])), tick))
}


for (a in 1:length(endurancemaleelo)){
  for(b in 1:length(endurancemaleelo[[a]])){
    for(c in 1:length(endurancemaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(endurancemaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      endurancemaledate=append(endurancemaledate, rep(as.character(names(endurancemaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(endurancemaleelo)){
  for(b in 1:length(endurancemaleelo[[a]])){
    for(c in 1:length(endurancemaleelo[[a]][[b]])){
      for(d in 1:length(endurancemaleelo[[a]][[b]][[c]])){
        endurancemaledistance=append(endurancemaledistance, as.character(names(endurancemaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(endurancemaleelo)){
  for(b in 1:length(endurancemaleelo[[a]])){
    for(c in 1:length(endurancemaleelo[[a]][[b]])){
      for(d in 1:length(endurancemaleelo[[a]][[b]][[c]])){
        endurancemaleeloscore=append(endurancemaleeloscore, as.double(endurancemaleelo[[a]][[b]][[c]][[d]]))}}}}


endurancemaleelodf <- data.frame(endurancemalenames, endurancemaledate, endurancemaledistance, endurancemaleeloscore)



enduranceladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

enduranceladynames = c()
blseason = c()
enduranceladydate = c()
enduranceladydistance = c()
enduranceladyeloscore = c()
for (a in 1:length(enduranceladyelo)){
  tick=0
  for(b in 1:length(enduranceladyelo[[a]])){
    for(c in 1:length(enduranceladyelo[[a]][[b]])){
      for(d in 1:length(enduranceladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #enduranceladynames2 = append(enduranceladynames2,as.character(names(enduranceladyelo[a])))
      }
    }
  }
  enduranceladynames = append(enduranceladynames, rep(as.character(names(enduranceladyelo[a])), tick))
}


for (a in 1:length(enduranceladyelo)){
  for(b in 1:length(enduranceladyelo[[a]])){
    for(c in 1:length(enduranceladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(enduranceladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      enduranceladydate=append(enduranceladydate, rep(as.character(names(enduranceladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(enduranceladyelo)){
  for(b in 1:length(enduranceladyelo[[a]])){
    for(c in 1:length(enduranceladyelo[[a]][[b]])){
      for(d in 1:length(enduranceladyelo[[a]][[b]][[c]])){
        enduranceladydistance=append(enduranceladydistance, as.character(names(enduranceladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(enduranceladyelo)){
  for(b in 1:length(enduranceladyelo[[a]])){
    for(c in 1:length(enduranceladyelo[[a]][[b]])){
      for(d in 1:length(enduranceladyelo[[a]][[b]][[c]])){
        enduranceladyeloscore=append(enduranceladyeloscore, as.double(enduranceladyelo[[a]][[b]][[c]][[d]]))}}}}


enduranceladyelodf <- data.frame(enduranceladynames, enduranceladydate, enduranceladydistance, enduranceladyeloscore)

saveendurancemalenames <- endurancemalenames
saveendurancemaledistance <- endurancemaledistance
saveendurancemaledate <- endurancemaledate
saveendurancemaleeloscore <- endurancemaleeloscore
saveenduranceladynames <- enduranceladynames
saveenduranceladydistance <- enduranceladydistance
saveenduranceladydate <- enduranceladydate
saveenduranceladyeloscore <- enduranceladyeloscore


endurancemalehighest <- endurancemaleelodf[order(-endurancemaleelodf$endurancemaleeloscore), ]
endurancemalehighest_ind <- endurancemalehighest[match(unique(endurancemalehighest$endurancemalenames), endurancemalehighest$endurancemalenames), ]
row.names(endurancemalehighest_ind) <- 1:length(endurancemalehighest_ind[,1])
endurancemalehighest_ind[1:25,]


enduranceladyhighest <- enduranceladyelodf[order(-enduranceladyelodf$enduranceladyeloscore), ]
enduranceladyhighest_ind <-enduranceladyhighest[match(unique(enduranceladyhighest$enduranceladynames), enduranceladyhighest$enduranceladynames), ]
row.names(enduranceladyhighest_ind) <- 1:length(enduranceladyhighest_ind[,1])
enduranceladyhighest_ind[1:25, ]

endurancemalelast_race <- endurancemaleelodf[endurancemaleelodf$endurancemaledate=="20200500",]
endurancemalelast_race[order(-endurancemalelast_race$endurancemaleeloscore),]

library(ggplot2)

jhk = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(endurancemaleeloscore), colour=endurancemalenames)) +geom_point()


bd = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = endurancemaleelodf[(endurancemaleelodf$endurancemalenames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(endurancemaleeloscore), colour=endurancemalenames)) +geom_point()


endurancemaleelodf[which(endurancemaleelodf$endurancemalenames=="Kevin Brochman"),]

jd = enduranceladyelodf[(enduranceladyelodf$enduranceladynames)=="Jessica Diggins", ]
kr = enduranceladyelodf[(enduranceladyelodf$enduranceladynames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(enduranceladyeloscore), colour=enduranceladynames)) +geom_point()

tj = enduranceladyelodf[(enduranceladyelodf$enduranceladynames)=="Therese Johaug", ]
mb = enduranceladyelodf[(enduranceladyelodf$enduranceladynames)=="Marit Bjørgen", ]
io = enduranceladyelodf[(enduranceladyelodf$enduranceladynames)=="Ingvild Flugstad Østberg", ]
hw = enduranceladyelodf[(enduranceladyelodf$enduranceladynames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(enduranceladyeloscore), colour=enduranceladynames)) +geom_point()


enduranceladyelodf[(enduranceladyelodf$enduranceladynames)=="Rosie Brennan", ]


endurancemalelast_race <- endurancemaleelodf[endurancemaleelodf$endurancemaledate=="20200500",]
endurancemalelast_race <- endurancemalelast_race[order(-endurancemalelast_race$endurancemaleeloscore),]
row.names(endurancemalelast_race) <- 1:length(endurancemalelast_race[,1])
endurancemalelast_race[1:25,]

enduranceladylast_race <- enduranceladyelodf[enduranceladyelodf$enduranceladydate=="20200500", ]
enduranceladylast_race <- enduranceladylast_race[order(-enduranceladylast_race$enduranceladyeloscore),]
row.names(enduranceladylast_race) <- 1:length(enduranceladylast_race[,1])
enduranceladylast_race[1:25,]


actendurancemaleelodfnames <- unique(endurancemaleelodf$endurancemalenames)
for(a in 1:length(actendurancemaleelodfnames)){
  actendurancemaleskier <- endurancemaleelodf[endurancemaleelodf$endurancemalenames==actendurancemaleelodfnames[a], ]
  #print(actendurancemaleskier)
  row.names(actendurancemaleskier) <- 1:length(actendurancemaleskier[,1])
  
  #Starting from the back, the last endurancemaledistance that is not zero.  Then that one plus one is 
  actendurancemaleraces <- as.numeric(as.character(row.names(actendurancemaleskier[which(actendurancemaleskier$endurancemaledistance!="0"), ])))
  actendurancemalelast <- actendurancemaleskier[1:actendurancemaleraces[length(actendurancemaleraces)]+1, ]
  
  if(a==1){
    row.names(actendurancemalelast) <- 1:length(actendurancemalelast[,1])
    actendurancemaleelodf<-actendurancemalelast
  }
  else{
    #print(length(actendurancemaleelodfnames)-a)
    row.names(actendurancemalelast) <- (length(actendurancemaleelodf[,1])+1):(length(actendurancemaleelodf[,1])+length(actendurancemalelast[,1]))
    actendurancemaleelodf <- rbind(actendurancemaleelodf, actendurancemalelast)
  }
}


actenduranceladyelodfnames <- unique(enduranceladyelodf$enduranceladynames)
for(a in 1:length(actenduranceladyelodfnames)){
  actenduranceladyskier <- enduranceladyelodf[enduranceladyelodf$enduranceladynames==actenduranceladyelodfnames[a], ]
  #print(actenduranceladyskier)
  row.names(actenduranceladyskier) <- 1:length(actenduranceladyskier[,1])
  
  #Starting from the back, the last enduranceladydistance that is not zero.  Then that one plus one is 
  actenduranceladyraces <- as.numeric(as.character(row.names(actenduranceladyskier[which(actenduranceladyskier$enduranceladydistance!="0"), ])))
  actenduranceladylast <- actenduranceladyskier[1:actenduranceladyraces[length(actenduranceladyraces)]+1, ]
  
  if(a==1){
    row.names(actenduranceladylast) <- 1:length(actenduranceladylast[,1])
    actenduranceladyelodf<-actenduranceladylast
  }
  else{
    #print(length(actenduranceladyelodfnames)-a)
    row.names(actenduranceladylast) <- (length(actenduranceladyelodf[,1])+1):(length(actenduranceladyelodf[,1])+length(actenduranceladylast[,1]))
    actenduranceladyelodf <- rbind(actenduranceladyelodf, actenduranceladylast)
  }
}
actendurancemalenation <- c()
actendurancemaleseason <- c()
for(a in unique(sort(actendurancemaleelodf$endurancemaledate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actendurancemaleseason <- append(actendurancemaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(actendurancemaleelodf[actendurancemaleelodf$endurancemaledate==a, 1])))
  }
  else{
    actendurancemaleseason <- append(actendurancemaleseason, rep(as.numeric(substr(a, 1, 4)), length(actendurancemaleelodf[actendurancemaleelodf$endurancemaledate==a, 1])))#myear <- order(-myear$mendurancemaleeloscore)
  }
}

for(a in unique(actendurancemaleelodf$endurancemalenames)){
  #print(a)
  actendurancemalenation <- append(actendurancemalenation, rep(mendf[mendf$Name==a,]$Nationality[length(mendf[mendf$Name==a,]$Nationality)], 
                                                               length(actendurancemaleelodf[actendurancemaleelodf$endurancemalenames==a,1])))
}
actendurancemaleelodf$endurancemaleseason <- NA
actendurancemaleelodf[order(as.character(actendurancemaleelodf$endurancemaledate)),]$endurancemaleseason <- actendurancemaleseason
actendurancemaleelodf$endurancemalenation <- actendurancemalenation

actenduranceladynation <- c()
actenduranceladyseason <- c()
for(a in unique(sort(actenduranceladyelodf$enduranceladydate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actenduranceladyseason <- append(actenduranceladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(actenduranceladyelodf[actenduranceladyelodf$enduranceladydate==a, 1])))
  }
  else{
    actenduranceladyseason <- append(actenduranceladyseason, rep(as.numeric(substr(a, 1, 4)), length(actenduranceladyelodf[actenduranceladyelodf$enduranceladydate==a, 1])))#myear <- order(-myear$mendurancemaleeloscore)
  }
}

for(a in unique(actenduranceladyelodf$enduranceladynames)){
  #print(a)
  actenduranceladynation <- append(actenduranceladynation, rep(enduranceladiesdf[enduranceladiesdf$Name==a,]$Nationality[length(enduranceladiesdf[enduranceladiesdf$Name==a,]$Nationality)], 
                                                               length(actenduranceladyelodf[actenduranceladyelodf$enduranceladynames==a,1])))
}
actenduranceladyelodf$enduranceladynation <- actenduranceladynation
#actenduranceladyelodf <- as.data.frame(actenduranceladyelodf)
actenduranceladyelodf$enduranceladyseason <- NA
actenduranceladyelodf[order(actenduranceladyelodf$enduranceladydate),]$enduranceladyseason <- (actenduranceladyseason)


actendurancemaleelodf[actendurancemaleelodf$endurancemalenames=="Johannes Høsflot Klæbo", ]
actenduranceladyelodf[actenduranceladyelodf$enduranceladynames=="Jessica Diggins", ]

actendurancemalelast_race <- actendurancemaleelodf[actendurancemaleelodf$endurancemaledate=="20200500",]
actendurancemalelast_race <- actendurancemalelast_race[order(-actendurancemalelast_race$endurancemaleeloscore),]
row.names(actendurancemalelast_race) <- 1:length(actendurancemalelast_race[,1])
#actendurancemalelast_race
actendurancemalelast_race[1:25,]
actendurancemalelast_race[actendurancemalelast_race$endurancemalenames=="Dario Cologna", ]

actenduranceladylast_race <- actenduranceladyelodf[actenduranceladyelodf$enduranceladydate=="20200500",]
actenduranceladylast_race <- actenduranceladylast_race[order(-actenduranceladylast_race$enduranceladyeloscore),]
row.names(actenduranceladylast_race) <- 1:length(actenduranceladylast_race[,1])
actenduranceladylast_race[1:25, ]
actenduranceladylast_race[actenduranceladylast_race$enduranceladynames=="Jessica Diggins", ]


endurancemaleseasonstandings <- actendurancemaleelodf[which(endsWith(as.character(actendurancemaleelodf$endurancemaledate), "0500") ), ]
endurancemaleseasonstandings <- endurancemaleseasonstandings[order((as.character(endurancemaleseasonstandings$endurancemaledate))), ]
endurancemalerank <- c()

for(a in unique(endurancemaleseasonstandings$endurancemaledate)){
  endurancemaleseasonstandings[endurancemaleseasonstandings$endurancemaledate==a, ] <- endurancemaleseasonstandings[endurancemaleseasonstandings$endurancemaledate==a, ][order(-endurancemaleseasonstandings[endurancemaleseasonstandings$endurancemaledate==a, ]$endurancemaleeloscore), ]
  endurancemalerank <- append(endurancemalerank, 1:length(endurancemaleseasonstandings[endurancemaleseasonstandings$endurancemaledate==a, 1]))
}
endurancemaleseasonstandings$endurancemalerank <- endurancemalerank
row.names(endurancemaleseasonstandings)<-1:length(endurancemaleseasonstandings[,1])
mwrong_names <- c()
# for(a in unique(endurancemaleseasonstandings$endurancemalenames)){
#   #endurancemaleseasonstandings[endurancemaleseasonstandings$endurancemalenames==a,] <- mendf
#   temp_nation <- mendf[mendf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     mwrong_names <- append(mwrong_names, a)
#   }
#   #if(length(endurancemaleseasonstandings[endurancemaleseasonstandings$endurancemalenames==a,1])>15){
#   #  print(a)
#   #}
#   endurancemaleseason_diff <- mendf[mendf$Name==a,]
#   #endurancemaleseasonnation <- append(endurancemaleseasonnation, mendf[mendf$Name==a,])
# }
# mwrong_names <- mwrong_names[! mwrong_names %in% c("Mikhail Devjatiarov", "Vladimir Smirnov", "Alexandr Uschkalenko", "Alexei Prokourorov", 
#                                 "Igor Badamchin", "Aleksandr Golubev", "Urmas Välbe", "Andrej Kirillov", "German Karatschewskij",
#                                 "Viatscheslav Plaksunov", "Gennadiy Lasutin", "Igor Obukhov", "Elmo Kassin", "Alexander Karatschewskij")]


endurancemaleseasonstandings[endurancemaleseasonstandings$endurancemaledate=="20200500", ]
 #View(endurancemaleseasonstandings[endurancemaleseasonstandings$endurancemalenames=="Petter Northug", ])
# View(mendf[mendf$Name=="Gunde Svan", ])
# View(endurancemaleseasonstandings[endurancemaleseasonstandings$endurancemalenation=="Norway",])
 View(endurancemaleseasonstandings)

enduranceladyseasonstandings <- actenduranceladyelodf[which(endsWith(as.character(actenduranceladyelodf$enduranceladydate), "0500") ), ]
enduranceladyseasonstandings <- enduranceladyseasonstandings[order((as.character(enduranceladyseasonstandings$enduranceladydate))), ]
enduranceladyrank <- c()
for(a in unique(enduranceladyseasonstandings$enduranceladydate)){
  enduranceladyseasonstandings[enduranceladyseasonstandings$enduranceladydate==a, ] <- 
    enduranceladyseasonstandings[enduranceladyseasonstandings$enduranceladydate==a, ][order(-enduranceladyseasonstandings[enduranceladyseasonstandings$enduranceladydate==a, ]$enduranceladyeloscore), ]
  #lyear <- order(-lyear$lendurancemaleeloscore)
  enduranceladyrank <- append(enduranceladyrank, 1:length(enduranceladyseasonstandings[enduranceladyseasonstandings$enduranceladydate==a, 1]))
}
enduranceladyseasonstandings$enduranceladyrank <- enduranceladyrank
row.names(enduranceladyseasonstandings)<-1:length(enduranceladyseasonstandings[,1])


lwrong_names <- c()
# for(a in unique(enduranceladyseasonstandings$enduranceladynames)){
#   #endurancemaleseasonstandings[endurancemaleseasonstandings$endurancemalenames==a,] <- enduranceladiesdf
#   temp_nation <- enduranceladiesdf[enduranceladiesdf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     lwrong_names <- append(lwrong_names, a)
#   }
#  # if(length(enduranceladyseasonstandings[enduranceladyseasonstandings$enduranceladynames==a,1])>20){
#   #  print(a)
#  # }
#   enduranceladyseason_diff <- enduranceladiesdf[enduranceladiesdf$Name==a,]
#   #endurancemaleseasonnation <- append(endurancemaleseasonnation, enduranceladiesdf[enduranceladiesdf$Name==a,])
# }


enduranceladyseasonstandings[enduranceladyseasonstandings$enduranceladydate=="20200500", ]
enduranceladyseasonstandings[enduranceladyseasonstandings$enduranceladynames=="Jessica Diggins", ]
# View(enduranceladiesdf[enduranceladiesdf$Name=="Lilia Vasilieva", ])
# View(enduranceladyseasonstandings[enduranceladyseasonstandings$enduranceladynation=="USA",])
# View(enduranceladyseasonstandings)

