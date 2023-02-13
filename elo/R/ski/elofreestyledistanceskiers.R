library(readxl)
freestyleendurancemen <- read_excel("~/ski/elo/excel365/all.xlsx", 
                                  sheet = "Men", col_names = FALSE, na = "NA")
freestyleenduranceladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                                     sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
freestyleendurancemendf <- data.frame(freestyleendurancemen)
names(freestyleendurancemendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
freestyleendurancemendf$Seasons = NA
freestyleendurancemendf[freestyleendurancemendf$Name=="Oddmund Jensen", ]
freestyleendurancemendf[2073,9] <- "Oddmund Jensen2"
#print(unique(freestyleendurancemendf$Discipline))
malefreestyleendurancedistances <-c("Sprint", "Stage") 
malefreestyleendurancediscipline <- c("F")
freestyleendurancemendf <- freestyleendurancemendf[!as.character(freestyleendurancemendf$Distance) %in% malefreestyleendurancedistances, ]
freestyleendurancemendf <- freestyleendurancemendf[as.character(freestyleendurancemendf$Discipline) %in% malefreestyleendurancediscipline,]
row.names(freestyleendurancemendf) <- 1:length(freestyleendurancemendf[,1])



freestyleenduranceladiesdf <- data.frame(freestyleenduranceladies)
names(freestyleenduranceladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
freestyleenduranceladiesdf$Seasons = NA
#print(unique(freestyleenduranceladiesdf$Discipline))
ladyfreestyleendurancedistances <- c("Sprint", "Stage")
ladyfreestyleendurancediscipline <- c("F")

freestyleenduranceladiesdf <- freestyleenduranceladiesdf[!as.character(freestyleenduranceladiesdf$Distance) %in% ladyfreestyleendurancedistances, ]
freestyleenduranceladiesdf <- freestyleenduranceladiesdf[as.character(freestyleenduranceladiesdf$Discipline) %in% ladyfreestyleendurancediscipline,]

row.names(freestyleenduranceladiesdf) <- 1:length(freestyleenduranceladiesdf[,1])
#freestyleenduranceladiesdf[4721, 9] <- "Tatjana Kuznetsova2"

for(a in 1:length(freestyleendurancemendf$Date)){
  if((as.double(substr(freestyleendurancemendf$Date[a], 5, 8)) > 1000) && as.double(substr(freestyleendurancemendf$Date[a],5,8)!=9999)){
    freestyleendurancemendf$Seasons[a] = as.character(as.double(substr(freestyleendurancemendf$Date[a], 1, 4))+1)
  }
  else{
    freestyleendurancemendf$Seasons[a] = as.character(as.double(substr(freestyleendurancemendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(freestyleenduranceladiesdf$Date)){
  if((as.double(substr(freestyleenduranceladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(freestyleenduranceladiesdf$Date[a],5,8)!=9999)){
    freestyleenduranceladiesdf$Seasons[a] = as.character(as.double(substr(freestyleenduranceladiesdf$Date[a], 1, 4))+1)
  }
  else{
    freestyleenduranceladiesdf$Seasons[a] = as.character(as.double(substr(freestyleenduranceladiesdf$Date[a], 1, 4)))
  }
}

freestyleendurancemaleraces = list()
freestyleenduranceladyraces = list()

for(a in 1:length(freestyleendurancemendf$Seasons)){
  if(freestyleendurancemendf$Seasons[a] %in% names(freestyleendurancemaleraces)==FALSE){
    freestyleendurancemaleraces[[freestyleendurancemendf$Seasons[a]]] <- list()
  }
  
  if(freestyleendurancemendf$Date[a] %in% names(freestyleendurancemaleraces[[freestyleendurancemendf$Seasons[a]]])==FALSE){
    freestyleendurancemaleraces[[freestyleendurancemendf$Seasons[a]]][[freestyleendurancemendf$Date[a]]] <- list()
  }
  if(freestyleendurancemendf$Distance[a] %in% names(freestyleendurancemaleraces[[freestyleendurancemendf$Seasons[a]]][[freestyleendurancemendf$Date[a]]])==FALSE){
    freestyleendurancemaleraces[[freestyleendurancemendf$Seasons[a]]][[freestyleendurancemendf$Date[a]]][[freestyleendurancemendf$Distance[a]]]<-list()
  }
  if(freestyleendurancemendf$Name[a] %in% names(freestyleendurancemaleraces[[freestyleendurancemendf$Seasons[a]]][[freestyleendurancemendf$Date[a]]][[freestyleendurancemendf$Distance[a]]])==FALSE){
    freestyleendurancemaleraces[[freestyleendurancemendf$Seasons[a]]][[freestyleendurancemendf$Date[a]]][[freestyleendurancemendf$Distance[a]]][[freestyleendurancemendf$Name[a]]]<-as.double(freestyleendurancemendf$Place[a])
  }
}

for(a in 1:length(freestyleenduranceladiesdf$Seasons)){
  if(freestyleenduranceladiesdf$Seasons[a] %in% names(freestyleenduranceladyraces)==FALSE){
    freestyleenduranceladyraces[[freestyleenduranceladiesdf$Seasons[a]]] <- list()
  }
  
  if(freestyleenduranceladiesdf$Date[a] %in% names(freestyleenduranceladyraces[[freestyleenduranceladiesdf$Seasons[a]]])==FALSE){
    freestyleenduranceladyraces[[freestyleenduranceladiesdf$Seasons[a]]][[freestyleenduranceladiesdf$Date[a]]] <- list()
  }
  if(freestyleenduranceladiesdf$Distance[a] %in% names(freestyleenduranceladyraces[[freestyleenduranceladiesdf$Seasons[a]]][[freestyleenduranceladiesdf$Date[a]]])==FALSE){
    freestyleenduranceladyraces[[freestyleenduranceladiesdf$Seasons[a]]][[freestyleenduranceladiesdf$Date[a]]][[freestyleenduranceladiesdf$Distance[a]]]<-list()
  }
  if(freestyleenduranceladiesdf$Name[a] %in% names(freestyleenduranceladyraces[[freestyleenduranceladiesdf$Seasons[a]]][[freestyleenduranceladiesdf$Date[a]]][[freestyleenduranceladiesdf$Distance[a]]])==FALSE){
    freestyleenduranceladyraces[[freestyleenduranceladiesdf$Seasons[a]]][[freestyleenduranceladiesdf$Date[a]]][[freestyleenduranceladiesdf$Distance[a]]][[freestyleenduranceladiesdf$Name[a]]]<-as.double(freestyleenduranceladiesdf$Place[a])
  }
}

savefreestyleendurancemaleraces<-freestyleendurancemaleraces
savefreestyleenduranceladyraces <- freestyleenduranceladyraces

#Initialize all skiers to have an Elo of 1300
freestyleendurancemaleelo = list()
freestyleenduranceladyelo = list()
for(a in 1:length(freestyleendurancemendf$Name)){
  if(freestyleendurancemendf$Name[a] %in% names(freestyleendurancemaleelo) == FALSE){
    freestyleendurancemaleelo[[freestyleendurancemendf$Name[a]]] <- list()
    freestyleendurancemaleelo[[freestyleendurancemendf$Name[a]]][["0000"]]<-list()
    freestyleendurancemaleelo[[freestyleendurancemendf$Name[a]]][["0000"]][["00000000"]]<-list()
    freestyleendurancemaleelo[[freestyleendurancemendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(freestyleenduranceladiesdf$Name)){
  if(freestyleenduranceladiesdf$Name[a] %in% names(freestyleenduranceladyelo) == FALSE){
    freestyleenduranceladyelo[[freestyleenduranceladiesdf$Name[a]]] <- list()
    freestyleenduranceladyelo[[freestyleenduranceladiesdf$Name[a]]][["0000"]]<-list()
    freestyleenduranceladyelo[[freestyleenduranceladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    freestyleenduranceladyelo[[freestyleenduranceladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savefreestyleendurancemaleelo<-freestyleendurancemaleelo
savefreestyleenduranceladyelo <- freestyleenduranceladyelo

freestyleendurancemaleelo<-savefreestyleendurancemaleelo

allelo <- c()
K=1
place_index = 1

for(z in 1:length(freestyleendurancemaleraces)){
  print(z)
  
  for(a in 1:length(freestyleendurancemaleraces[[z]])){
    
    
    for(b in 1:length(freestyleendurancemaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(freestyleendurancemaleraces[[z]][[a]][[b]])){
        lastp = freestyleendurancemaleelo[[names(freestyleendurancemaleraces[[z]][[a]][[b]][c])]][[length(freestyleendurancemaleelo[[names(freestyleendurancemaleraces[[z]][[a]][[b]][c])]])]][[length(freestyleendurancemaleelo[[names(freestyleendurancemaleraces[[z]][[a]][[b]][c])]][[length(freestyleendurancemaleelo[[names(freestyleendurancemaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(freestyleendurancemaleraces[[z]][[a]][[b]]), as.double(freestyleendurancemendf$Place[place_index:(place_index+length(names(freestyleendurancemaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(freestyleendurancemaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(freestyleendurancemaleraces[[z]][[a]][[b]][c])
        current_year <- names(freestyleendurancemaleraces[z])
        if(current_year %in% names(freestyleendurancemaleelo[[names(freestyleendurancemaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          freestyleendurancemaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(freestyleendurancemaleraces[[z]][a])
        if(current_date %in% names(freestyleendurancemaleelo[[names(freestyleendurancemaleraces[[z]][[a]][[b]][c])]][[names(freestyleendurancemaleraces[z])]])==FALSE){
          freestyleendurancemaleelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(freestyleendurancemaleraces[[z]][[a]][b])
        if(current_race %in% names(freestyleendurancemaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          freestyleendurancemaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(freestyleendurancemaleelo)){
    if(length(freestyleendurancemaleelo[[d]])>1){
      plastp = freestyleendurancemaleelo[[d]][[length(freestyleendurancemaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #freestyleendurancemaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      freestyleendurancemaleelo[[d]][[current_year]][[current_date]] <- list()
      freestyleendurancemaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)
freestyleenduranceladyelo <- savefreestyleenduranceladyelo
place_index = 1
allelo <- c()
for(z in 1:length(freestyleenduranceladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(freestyleenduranceladyraces[[z]])){
    
    
    for(b in 1:length(freestyleenduranceladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(freestyleenduranceladyraces[[z]][[a]][[b]])){
        lastp = freestyleenduranceladyelo[[names(freestyleenduranceladyraces[[z]][[a]][[b]][c])]][[length(freestyleenduranceladyelo[[names(freestyleenduranceladyraces[[z]][[a]][[b]][c])]])]][[length(freestyleenduranceladyelo[[names(freestyleenduranceladyraces[[z]][[a]][[b]][c])]][[length(freestyleenduranceladyelo[[names(freestyleenduranceladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(freestyleenduranceladyraces[[z]][[a]][[b]]), as.double(freestyleenduranceladiesdf$Place[place_index:(place_index+length(names(freestyleenduranceladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(freestyleenduranceladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(freestyleenduranceladyraces[[z]][[a]][[b]][c])
        current_year <- names(freestyleenduranceladyraces[z])
        if(current_year %in% names(freestyleenduranceladyelo[[names(freestyleenduranceladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          freestyleenduranceladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(freestyleenduranceladyraces[[z]][a])
        if(current_date %in% names(freestyleenduranceladyelo[[names(freestyleenduranceladyraces[[z]][[a]][[b]][c])]][[names(freestyleenduranceladyraces[z])]])==FALSE){
          freestyleenduranceladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(freestyleenduranceladyraces[[z]][[a]][b])
        if(current_race %in% names(freestyleenduranceladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          freestyleenduranceladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(freestyleenduranceladyelo)){
    if(length(freestyleenduranceladyelo[[d]])>1){
      plastp = freestyleenduranceladyelo[[d]][[length(freestyleenduranceladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #freestyleenduranceladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      freestyleenduranceladyelo[[d]][[current_year]][[current_date]] <- list()
      freestyleenduranceladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)


freestyleendurancemaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

freestyleendurancemalenames = c()
blseason = c()
freestyleendurancemaledate = c()
freestyleendurancemaledistance = c()
freestyleendurancemaleeloscore = c()
for (a in 1:length(freestyleendurancemaleelo)){
  tick=0
  for(b in 1:length(freestyleendurancemaleelo[[a]])){
    for(c in 1:length(freestyleendurancemaleelo[[a]][[b]])){
      for(d in 1:length(freestyleendurancemaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #freestyleendurancemalenames2 = append(freestyleendurancemalenames2,as.character(names(freestyleendurancemaleelo[a])))
      }
    }
  }
  freestyleendurancemalenames = append(freestyleendurancemalenames, rep(as.character(names(freestyleendurancemaleelo[a])), tick))
}


for (a in 1:length(freestyleendurancemaleelo)){
  for(b in 1:length(freestyleendurancemaleelo[[a]])){
    for(c in 1:length(freestyleendurancemaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(freestyleendurancemaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      freestyleendurancemaledate=append(freestyleendurancemaledate, rep(as.character(names(freestyleendurancemaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(freestyleendurancemaleelo)){
  for(b in 1:length(freestyleendurancemaleelo[[a]])){
    for(c in 1:length(freestyleendurancemaleelo[[a]][[b]])){
      for(d in 1:length(freestyleendurancemaleelo[[a]][[b]][[c]])){
        freestyleendurancemaledistance=append(freestyleendurancemaledistance, as.character(names(freestyleendurancemaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(freestyleendurancemaleelo)){
  for(b in 1:length(freestyleendurancemaleelo[[a]])){
    for(c in 1:length(freestyleendurancemaleelo[[a]][[b]])){
      for(d in 1:length(freestyleendurancemaleelo[[a]][[b]][[c]])){
        freestyleendurancemaleeloscore=append(freestyleendurancemaleeloscore, as.double(freestyleendurancemaleelo[[a]][[b]][[c]][[d]]))}}}}


freestyleendurancemaleelodf <- data.frame(freestyleendurancemalenames, freestyleendurancemaledate, freestyleendurancemaledistance, freestyleendurancemaleeloscore)



freestyleenduranceladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

freestyleenduranceladynames = c()
blseason = c()
freestyleenduranceladydate = c()
freestyleenduranceladydistance = c()
freestyleenduranceladyeloscore = c()
for (a in 1:length(freestyleenduranceladyelo)){
  tick=0
  for(b in 1:length(freestyleenduranceladyelo[[a]])){
    for(c in 1:length(freestyleenduranceladyelo[[a]][[b]])){
      for(d in 1:length(freestyleenduranceladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #freestyleenduranceladynames2 = append(freestyleenduranceladynames2,as.character(names(freestyleenduranceladyelo[a])))
      }
    }
  }
  freestyleenduranceladynames = append(freestyleenduranceladynames, rep(as.character(names(freestyleenduranceladyelo[a])), tick))
}


for (a in 1:length(freestyleenduranceladyelo)){
  for(b in 1:length(freestyleenduranceladyelo[[a]])){
    for(c in 1:length(freestyleenduranceladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(freestyleenduranceladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      freestyleenduranceladydate=append(freestyleenduranceladydate, rep(as.character(names(freestyleenduranceladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(freestyleenduranceladyelo)){
  for(b in 1:length(freestyleenduranceladyelo[[a]])){
    for(c in 1:length(freestyleenduranceladyelo[[a]][[b]])){
      for(d in 1:length(freestyleenduranceladyelo[[a]][[b]][[c]])){
        freestyleenduranceladydistance=append(freestyleenduranceladydistance, as.character(names(freestyleenduranceladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(freestyleenduranceladyelo)){
  for(b in 1:length(freestyleenduranceladyelo[[a]])){
    for(c in 1:length(freestyleenduranceladyelo[[a]][[b]])){
      for(d in 1:length(freestyleenduranceladyelo[[a]][[b]][[c]])){
        freestyleenduranceladyeloscore=append(freestyleenduranceladyeloscore, as.double(freestyleenduranceladyelo[[a]][[b]][[c]][[d]]))}}}}


freestyleenduranceladyelodf <- data.frame(freestyleenduranceladynames, freestyleenduranceladydate, freestyleenduranceladydistance, freestyleenduranceladyeloscore)

savefreestyleendurancemalenames <- freestyleendurancemalenames
savefreestyleendurancemaledistance <- freestyleendurancemaledistance
savefreestyleendurancemaledate <- freestyleendurancemaledate
savefreestyleendurancemaleeloscore <- freestyleendurancemaleeloscore
savefreestyleenduranceladynames <- freestyleenduranceladynames
savefreestyleenduranceladydistance <- freestyleenduranceladydistance
savefreestyleenduranceladydate <- freestyleenduranceladydate
savefreestyleenduranceladyeloscore <- freestyleenduranceladyeloscore


freestyleendurancemalehighest <- freestyleendurancemaleelodf[order(-freestyleendurancemaleelodf$freestyleendurancemaleeloscore), ]
freestyleendurancemalehighest_ind <- freestyleendurancemalehighest[match(unique(freestyleendurancemalehighest$freestyleendurancemalenames), freestyleendurancemalehighest$freestyleendurancemalenames), ]
row.names(freestyleendurancemalehighest_ind) <- 1:length(freestyleendurancemalehighest_ind[,1])
freestyleendurancemalehighest_ind[1:25,]


freestyleenduranceladyhighest <- freestyleenduranceladyelodf[order(-freestyleenduranceladyelodf$freestyleenduranceladyeloscore), ]
freestyleenduranceladyhighest_ind <-freestyleenduranceladyhighest[match(unique(freestyleenduranceladyhighest$freestyleenduranceladynames), freestyleenduranceladyhighest$freestyleenduranceladynames), ]
row.names(freestyleenduranceladyhighest_ind) <- 1:length(freestyleenduranceladyhighest_ind[,1])
freestyleenduranceladyhighest_ind[1:25, ]

freestyleendurancemalelast_race <- freestyleendurancemaleelodf[freestyleendurancemaleelodf$freestyleendurancemaledate=="20200500",]
freestyleendurancemalelast_race[order(-freestyleendurancemalelast_race$freestyleendurancemaleeloscore),]

library(ggplot2)

jhk = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(freestyleendurancemaleeloscore), colour=freestyleendurancemalenames)) +geom_point()


bd = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = freestyleendurancemaleelodf[(freestyleendurancemaleelodf$freestyleendurancemalenames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(freestyleendurancemaleeloscore), colour=freestyleendurancemalenames)) +geom_point()


freestyleendurancemaleelodf[which(freestyleendurancemaleelodf$freestyleendurancemalenames=="Kevin Brochman"),]

jd = freestyleenduranceladyelodf[(freestyleenduranceladyelodf$freestyleenduranceladynames)=="Jessica Diggins", ]
kr = freestyleenduranceladyelodf[(freestyleenduranceladyelodf$freestyleenduranceladynames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(freestyleenduranceladyeloscore), colour=freestyleenduranceladynames)) +geom_point()

tj = freestyleenduranceladyelodf[(freestyleenduranceladyelodf$freestyleenduranceladynames)=="Therese Johaug", ]
mb = freestyleenduranceladyelodf[(freestyleenduranceladyelodf$freestyleenduranceladynames)=="Marit Bjørgen", ]
io = freestyleenduranceladyelodf[(freestyleenduranceladyelodf$freestyleenduranceladynames)=="Ingvild Flugstad Østberg", ]
hw = freestyleenduranceladyelodf[(freestyleenduranceladyelodf$freestyleenduranceladynames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(freestyleenduranceladyeloscore), colour=freestyleenduranceladynames)) +geom_point()


freestyleenduranceladyelodf[(freestyleenduranceladyelodf$freestyleenduranceladynames)=="Rosie Brennan", ]


freestyleendurancemalelast_race <- freestyleendurancemaleelodf[freestyleendurancemaleelodf$freestyleendurancemaledate=="20200500",]
freestyleendurancemalelast_race <- freestyleendurancemalelast_race[order(-freestyleendurancemalelast_race$freestyleendurancemaleeloscore),]
row.names(freestyleendurancemalelast_race) <- 1:length(freestyleendurancemalelast_race[,1])
freestyleendurancemalelast_race[1:25,]

freestyleenduranceladylast_race <- freestyleenduranceladyelodf[freestyleenduranceladyelodf$freestyleenduranceladydate=="20200500", ]
freestyleenduranceladylast_race <- freestyleenduranceladylast_race[order(-freestyleenduranceladylast_race$freestyleenduranceladyeloscore),]
row.names(freestyleenduranceladylast_race) <- 1:length(freestyleenduranceladylast_race[,1])
freestyleenduranceladylast_race[1:25,]


actfreestyleendurancemaleelodfnames <- unique(freestyleendurancemaleelodf$freestyleendurancemalenames)
for(a in 1:length(actfreestyleendurancemaleelodfnames)){
  actfreestyleendurancemaleskier <- freestyleendurancemaleelodf[freestyleendurancemaleelodf$freestyleendurancemalenames==actfreestyleendurancemaleelodfnames[a], ]
  #print(actfreestyleendurancemaleskier)
  row.names(actfreestyleendurancemaleskier) <- 1:length(actfreestyleendurancemaleskier[,1])
  
  #Starting from the back, the last freestyleendurancemaledistance that is not zero.  Then that one plus one is 
  actfreestyleendurancemaleraces <- as.numeric(as.character(row.names(actfreestyleendurancemaleskier[which(actfreestyleendurancemaleskier$freestyleendurancemaledistance!="0"), ])))
  actfreestyleendurancemalelast <- actfreestyleendurancemaleskier[1:actfreestyleendurancemaleraces[length(actfreestyleendurancemaleraces)]+1, ]
  
  if(a==1){
    row.names(actfreestyleendurancemalelast) <- 1:length(actfreestyleendurancemalelast[,1])
    actfreestyleendurancemaleelodf<-actfreestyleendurancemalelast
  }
  else{
    #print(length(actfreestyleendurancemaleelodfnames)-a)
    row.names(actfreestyleendurancemalelast) <- (length(actfreestyleendurancemaleelodf[,1])+1):(length(actfreestyleendurancemaleelodf[,1])+length(actfreestyleendurancemalelast[,1]))
    actfreestyleendurancemaleelodf <- rbind(actfreestyleendurancemaleelodf, actfreestyleendurancemalelast)
  }
}


actfreestyleenduranceladyelodfnames <- unique(freestyleenduranceladyelodf$freestyleenduranceladynames)
for(a in 1:length(actfreestyleenduranceladyelodfnames)){
  actfreestyleenduranceladyskier <- freestyleenduranceladyelodf[freestyleenduranceladyelodf$freestyleenduranceladynames==actfreestyleenduranceladyelodfnames[a], ]
  #print(actfreestyleenduranceladyskier)
  row.names(actfreestyleenduranceladyskier) <- 1:length(actfreestyleenduranceladyskier[,1])
  
  #Starting from the back, the last freestyleenduranceladydistance that is not zero.  Then that one plus one is 
  actfreestyleenduranceladyraces <- as.numeric(as.character(row.names(actfreestyleenduranceladyskier[which(actfreestyleenduranceladyskier$freestyleenduranceladydistance!="0"), ])))
  actfreestyleenduranceladylast <- actfreestyleenduranceladyskier[1:actfreestyleenduranceladyraces[length(actfreestyleenduranceladyraces)]+1, ]
  
  if(a==1){
    row.names(actfreestyleenduranceladylast) <- 1:length(actfreestyleenduranceladylast[,1])
    actfreestyleenduranceladyelodf<-actfreestyleenduranceladylast
  }
  else{
    #print(length(actfreestyleenduranceladyelodfnames)-a)
    row.names(actfreestyleenduranceladylast) <- (length(actfreestyleenduranceladyelodf[,1])+1):(length(actfreestyleenduranceladyelodf[,1])+length(actfreestyleenduranceladylast[,1]))
    actfreestyleenduranceladyelodf <- rbind(actfreestyleenduranceladyelodf, actfreestyleenduranceladylast)
  }
}
actfreestyleendurancemalenation <- c()
actfreestyleendurancemaleseason <- c()
for(a in unique(sort(actfreestyleendurancemaleelodf$freestyleendurancemaledate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actfreestyleendurancemaleseason <- append(actfreestyleendurancemaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(actfreestyleendurancemaleelodf[actfreestyleendurancemaleelodf$freestyleendurancemaledate==a, 1])))
  }
  else{
    actfreestyleendurancemaleseason <- append(actfreestyleendurancemaleseason, rep(as.numeric(substr(a, 1, 4)), length(actfreestyleendurancemaleelodf[actfreestyleendurancemaleelodf$freestyleendurancemaledate==a, 1])))#myear <- order(-myear$mfreestyleendurancemaleeloscore)
  }
}

for(a in unique(actfreestyleendurancemaleelodf$freestyleendurancemalenames)){
  #print(a)
  actfreestyleendurancemalenation <- append(actfreestyleendurancemalenation, rep(mendf[mendf$Name==a,]$Nationality[length(mendf[mendf$Name==a,]$Nationality)], 
                                                                             length(actfreestyleendurancemaleelodf[actfreestyleendurancemaleelodf$freestyleendurancemalenames==a,1])))
}
actfreestyleendurancemaleelodf$freestyleendurancemaleseason <- NA
actfreestyleendurancemaleelodf[order(as.character(actfreestyleendurancemaleelodf$freestyleendurancemaledate)),]$freestyleendurancemaleseason <- actfreestyleendurancemaleseason
actfreestyleendurancemaleelodf$freestyleendurancemalenation <- actfreestyleendurancemalenation

actfreestyleenduranceladynation <- c()
actfreestyleenduranceladyseason <- c()
for(a in unique(sort(actfreestyleenduranceladyelodf$freestyleenduranceladydate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actfreestyleenduranceladyseason <- append(actfreestyleenduranceladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(actfreestyleenduranceladyelodf[actfreestyleenduranceladyelodf$freestyleenduranceladydate==a, 1])))
  }
  else{
    actfreestyleenduranceladyseason <- append(actfreestyleenduranceladyseason, rep(as.numeric(substr(a, 1, 4)), length(actfreestyleenduranceladyelodf[actfreestyleenduranceladyelodf$freestyleenduranceladydate==a, 1])))#myear <- order(-myear$mfreestyleendurancemaleeloscore)
  }
}

for(a in unique(actfreestyleenduranceladyelodf$freestyleenduranceladynames)){
  #print(a)
  actfreestyleenduranceladynation <- append(actfreestyleenduranceladynation, rep(freestyleenduranceladiesdf[freestyleenduranceladiesdf$Name==a,]$Nationality[length(freestyleenduranceladiesdf[freestyleenduranceladiesdf$Name==a,]$Nationality)], 
                                                                             length(actfreestyleenduranceladyelodf[actfreestyleenduranceladyelodf$freestyleenduranceladynames==a,1])))
}
actfreestyleenduranceladyelodf$freestyleenduranceladynation <- actfreestyleenduranceladynation
#actfreestyleenduranceladyelodf <- as.data.frame(actfreestyleenduranceladyelodf)
actfreestyleenduranceladyelodf$freestyleenduranceladyseason <- NA
actfreestyleenduranceladyelodf[order(actfreestyleenduranceladyelodf$freestyleenduranceladydate),]$freestyleenduranceladyseason <- (actfreestyleenduranceladyseason)


actfreestyleendurancemaleelodf[actfreestyleendurancemaleelodf$freestyleendurancemalenames=="Johannes Høsflot Klæbo", ]
actfreestyleenduranceladyelodf[actfreestyleenduranceladyelodf$freestyleenduranceladynames=="Jessica Diggins", ]

actfreestyleendurancemalelast_race <- actfreestyleendurancemaleelodf[actfreestyleendurancemaleelodf$freestyleendurancemaledate=="20200500",]
actfreestyleendurancemalelast_race <- actfreestyleendurancemalelast_race[order(-actfreestyleendurancemalelast_race$freestyleendurancemaleeloscore),]
row.names(actfreestyleendurancemalelast_race) <- 1:length(actfreestyleendurancemalelast_race[,1])
#actfreestyleendurancemalelast_race
actfreestyleendurancemalelast_race[1:25,]
actfreestyleendurancemalelast_race[actfreestyleendurancemalelast_race$freestyleendurancemalenames=="Dario Cologna", ]

actfreestyleenduranceladylast_race <- actfreestyleenduranceladyelodf[actfreestyleenduranceladyelodf$freestyleenduranceladydate=="20200500",]
actfreestyleenduranceladylast_race <- actfreestyleenduranceladylast_race[order(-actfreestyleenduranceladylast_race$freestyleenduranceladyeloscore),]
row.names(actfreestyleenduranceladylast_race) <- 1:length(actfreestyleenduranceladylast_race[,1])
actfreestyleenduranceladylast_race[1:25, ]
actfreestyleenduranceladylast_race[actfreestyleenduranceladylast_race$freestyleenduranceladynames=="Jessica Diggins", ]


freestyleendurancemaleseasonstandings <- actfreestyleendurancemaleelodf[which(endsWith(as.character(actfreestyleendurancemaleelodf$freestyleendurancemaledate), "0500") ), ]
freestyleendurancemaleseasonstandings <- freestyleendurancemaleseasonstandings[order((as.character(freestyleendurancemaleseasonstandings$freestyleendurancemaledate))), ]
freestyleendurancemalerank <- c()

for(a in unique(freestyleendurancemaleseasonstandings$freestyleendurancemaledate)){
  freestyleendurancemaleseasonstandings[freestyleendurancemaleseasonstandings$freestyleendurancemaledate==a, ] <- freestyleendurancemaleseasonstandings[freestyleendurancemaleseasonstandings$freestyleendurancemaledate==a, ][order(-freestyleendurancemaleseasonstandings[freestyleendurancemaleseasonstandings$freestyleendurancemaledate==a, ]$freestyleendurancemaleeloscore), ]
  freestyleendurancemalerank <- append(freestyleendurancemalerank, 1:length(freestyleendurancemaleseasonstandings[freestyleendurancemaleseasonstandings$freestyleendurancemaledate==a, 1]))
}
freestyleendurancemaleseasonstandings$freestyleendurancemalerank <- freestyleendurancemalerank
row.names(freestyleendurancemaleseasonstandings)<-1:length(freestyleendurancemaleseasonstandings[,1])
mwrong_names <- c()
# for(a in unique(freestyleendurancemaleseasonstandings$freestyleendurancemalenames)){
#   #freestyleendurancemaleseasonstandings[freestyleendurancemaleseasonstandings$freestyleendurancemalenames==a,] <- mendf
#   temp_nation <- mendf[mendf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     mwrong_names <- append(mwrong_names, a)
#   }
#   #if(length(freestyleendurancemaleseasonstandings[freestyleendurancemaleseasonstandings$freestyleendurancemalenames==a,1])>15){
#   #  print(a)
#   #}
#   freestyleendurancemaleseason_diff <- mendf[mendf$Name==a,]
#   #freestyleendurancemaleseasonnation <- append(freestyleendurancemaleseasonnation, mendf[mendf$Name==a,])
# }
# mwrong_names <- mwrong_names[! mwrong_names %in% c("Mikhail Devjatiarov", "Vladimir Smirnov", "Alexandr Uschkalenko", "Alexei Prokourorov", 
#                                 "Igor Badamchin", "Aleksandr Golubev", "Urmas Välbe", "Andrej Kirillov", "German Karatschewskij",
#                                 "Viatscheslav Plaksunov", "Gennadiy Lasutin", "Igor Obukhov", "Elmo Kassin", "Alexander Karatschewskij")]


freestyleendurancemaleseasonstandings[freestyleendurancemaleseasonstandings$freestyleendurancemaledate=="20200500", ]
# View(freestyleendurancemaleseasonstandings[freestyleendurancemaleseasonstandings$freestyleendurancemalenames=="Gunde Svan", ])
# View(mendf[mendf$Name=="Gunde Svan", ])
# View(freestyleendurancemaleseasonstandings[freestyleendurancemaleseasonstandings$freestyleendurancemalenation=="Norway",])
# View(freestyleendurancemaleseasonstandings)

freestyleenduranceladyseasonstandings <- actfreestyleenduranceladyelodf[which(endsWith(as.character(actfreestyleenduranceladyelodf$freestyleenduranceladydate), "0500") ), ]
freestyleenduranceladyseasonstandings <- freestyleenduranceladyseasonstandings[order((as.character(freestyleenduranceladyseasonstandings$freestyleenduranceladydate))), ]
freestyleenduranceladyrank <- c()
for(a in unique(freestyleenduranceladyseasonstandings$freestyleenduranceladydate)){
  freestyleenduranceladyseasonstandings[freestyleenduranceladyseasonstandings$freestyleenduranceladydate==a, ] <- 
    freestyleenduranceladyseasonstandings[freestyleenduranceladyseasonstandings$freestyleenduranceladydate==a, ][order(-freestyleenduranceladyseasonstandings[freestyleenduranceladyseasonstandings$freestyleenduranceladydate==a, ]$freestyleenduranceladyeloscore), ]
  #lyear <- order(-lyear$lfreestyleendurancemaleeloscore)
  freestyleenduranceladyrank <- append(freestyleenduranceladyrank, 1:length(freestyleenduranceladyseasonstandings[freestyleenduranceladyseasonstandings$freestyleenduranceladydate==a, 1]))
}
freestyleenduranceladyseasonstandings$freestyleenduranceladyrank <- freestyleenduranceladyrank
row.names(freestyleenduranceladyseasonstandings)<-1:length(freestyleenduranceladyseasonstandings[,1])


lwrong_names <- c()
# for(a in unique(freestyleenduranceladyseasonstandings$freestyleenduranceladynames)){
#   #freestyleendurancemaleseasonstandings[freestyleendurancemaleseasonstandings$freestyleendurancemalenames==a,] <- freestyleenduranceladiesdf
#   temp_nation <- freestyleenduranceladiesdf[freestyleenduranceladiesdf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     lwrong_names <- append(lwrong_names, a)
#   }
#  # if(length(freestyleenduranceladyseasonstandings[freestyleenduranceladyseasonstandings$freestyleenduranceladynames==a,1])>20){
#   #  print(a)
#  # }
#   freestyleenduranceladyseason_diff <- freestyleenduranceladiesdf[freestyleenduranceladiesdf$Name==a,]
#   #freestyleendurancemaleseasonnation <- append(freestyleendurancemaleseasonnation, freestyleenduranceladiesdf[freestyleenduranceladiesdf$Name==a,])
# }


freestyleenduranceladyseasonstandings[freestyleenduranceladyseasonstandings$freestyleenduranceladydate=="20200500", ]
freestyleenduranceladyseasonstandings[freestyleenduranceladyseasonstandings$freestyleenduranceladynames=="Jessica Diggins", ]
# View(freestyleenduranceladiesdf[freestyleenduranceladiesdf$Name=="Lilia Vasilieva", ])
# View(freestyleenduranceladyseasonstandings[freestyleenduranceladyseasonstandings$freestyleenduranceladynation=="USA",])
# View(freestyleenduranceladyseasonstandings)

