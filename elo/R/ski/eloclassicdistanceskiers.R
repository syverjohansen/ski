library(readxl)
classicendurancemen <- read_excel("~/ski/elo/excel365/all.xlsx", 
                           sheet = "Men", col_names = FALSE, na = "NA")
classicenduranceladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                              sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
classicendurancemendf <- data.frame(classicendurancemen)
names(classicendurancemendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
classicendurancemendf$Seasons = NA
classicendurancemendf[classicendurancemendf$Name=="Oddmund Jensen", ]
classicendurancemendf[2073,9] <- "Oddmund Jensen2"
#print(unique(classicendurancemendf$Discipline))
maleclassicendurancedistances <-c("Sprint", "Stage") 
maleclassicendurancediscipline <- c("C", "N/A")
classicendurancemendf <- classicendurancemendf[!as.character(classicendurancemendf$Distance) %in% maleclassicendurancedistances, ]
classicendurancemendf <- classicendurancemendf[as.character(classicendurancemendf$Discipline) %in% maleclassicendurancediscipline,]
row.names(classicendurancemendf) <- 1:length(classicendurancemendf[,1])



classicenduranceladiesdf <- data.frame(classicenduranceladies)
names(classicenduranceladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
classicenduranceladiesdf$Seasons = NA
#print(unique(classicenduranceladiesdf$Discipline))
ladyclassicendurancedistances <- c("Sprint", "Stage")
ladyclassicendurancediscipline <- c("C", "NA", "N/A")

classicenduranceladiesdf <- classicenduranceladiesdf[!as.character(classicenduranceladiesdf$Distance) %in% ladyclassicendurancedistances, ]
classicenduranceladiesdf <- classicenduranceladiesdf[as.character(classicenduranceladiesdf$Discipline) %in% ladyclassicendurancediscipline,]

row.names(classicenduranceladiesdf) <- 1:length(classicenduranceladiesdf[,1])
#classicenduranceladiesdf[4721, 9] <- "Tatjana Kuznetsova2"

for(a in 1:length(classicendurancemendf$Date)){
  if((as.double(substr(classicendurancemendf$Date[a], 5, 8)) > 1000) && as.double(substr(classicendurancemendf$Date[a],5,8)!=9999)){
    classicendurancemendf$Seasons[a] = as.character(as.double(substr(classicendurancemendf$Date[a], 1, 4))+1)
  }
  else{
    classicendurancemendf$Seasons[a] = as.character(as.double(substr(classicendurancemendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(classicenduranceladiesdf$Date)){
  if((as.double(substr(classicenduranceladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(classicenduranceladiesdf$Date[a],5,8)!=9999)){
    classicenduranceladiesdf$Seasons[a] = as.character(as.double(substr(classicenduranceladiesdf$Date[a], 1, 4))+1)
  }
  else{
    classicenduranceladiesdf$Seasons[a] = as.character(as.double(substr(classicenduranceladiesdf$Date[a], 1, 4)))
  }
}

classicendurancemaleraces = list()
classicenduranceladyraces = list()

for(a in 1:length(classicendurancemendf$Seasons)){
  if(classicendurancemendf$Seasons[a] %in% names(classicendurancemaleraces)==FALSE){
    classicendurancemaleraces[[classicendurancemendf$Seasons[a]]] <- list()
  }
  
  if(classicendurancemendf$Date[a] %in% names(classicendurancemaleraces[[classicendurancemendf$Seasons[a]]])==FALSE){
    classicendurancemaleraces[[classicendurancemendf$Seasons[a]]][[classicendurancemendf$Date[a]]] <- list()
  }
  if(classicendurancemendf$Distance[a] %in% names(classicendurancemaleraces[[classicendurancemendf$Seasons[a]]][[classicendurancemendf$Date[a]]])==FALSE){
    classicendurancemaleraces[[classicendurancemendf$Seasons[a]]][[classicendurancemendf$Date[a]]][[classicendurancemendf$Distance[a]]]<-list()
  }
  if(classicendurancemendf$Name[a] %in% names(classicendurancemaleraces[[classicendurancemendf$Seasons[a]]][[classicendurancemendf$Date[a]]][[classicendurancemendf$Distance[a]]])==FALSE){
    classicendurancemaleraces[[classicendurancemendf$Seasons[a]]][[classicendurancemendf$Date[a]]][[classicendurancemendf$Distance[a]]][[classicendurancemendf$Name[a]]]<-as.double(classicendurancemendf$Place[a])
  }
}

for(a in 1:length(classicenduranceladiesdf$Seasons)){
  if(classicenduranceladiesdf$Seasons[a] %in% names(classicenduranceladyraces)==FALSE){
    classicenduranceladyraces[[classicenduranceladiesdf$Seasons[a]]] <- list()
  }
  
  if(classicenduranceladiesdf$Date[a] %in% names(classicenduranceladyraces[[classicenduranceladiesdf$Seasons[a]]])==FALSE){
    classicenduranceladyraces[[classicenduranceladiesdf$Seasons[a]]][[classicenduranceladiesdf$Date[a]]] <- list()
  }
  if(classicenduranceladiesdf$Distance[a] %in% names(classicenduranceladyraces[[classicenduranceladiesdf$Seasons[a]]][[classicenduranceladiesdf$Date[a]]])==FALSE){
    classicenduranceladyraces[[classicenduranceladiesdf$Seasons[a]]][[classicenduranceladiesdf$Date[a]]][[classicenduranceladiesdf$Distance[a]]]<-list()
  }
  if(classicenduranceladiesdf$Name[a] %in% names(classicenduranceladyraces[[classicenduranceladiesdf$Seasons[a]]][[classicenduranceladiesdf$Date[a]]][[classicenduranceladiesdf$Distance[a]]])==FALSE){
    classicenduranceladyraces[[classicenduranceladiesdf$Seasons[a]]][[classicenduranceladiesdf$Date[a]]][[classicenduranceladiesdf$Distance[a]]][[classicenduranceladiesdf$Name[a]]]<-as.double(classicenduranceladiesdf$Place[a])
  }
}

saveclassicendurancemaleraces<-classicendurancemaleraces
saveclassicenduranceladyraces <- classicenduranceladyraces

#Initialize all skiers to have an Elo of 1300
classicendurancemaleelo = list()
classicenduranceladyelo = list()
for(a in 1:length(classicendurancemendf$Name)){
  if(classicendurancemendf$Name[a] %in% names(classicendurancemaleelo) == FALSE){
    classicendurancemaleelo[[classicendurancemendf$Name[a]]] <- list()
    classicendurancemaleelo[[classicendurancemendf$Name[a]]][["0000"]]<-list()
    classicendurancemaleelo[[classicendurancemendf$Name[a]]][["0000"]][["00000000"]]<-list()
    classicendurancemaleelo[[classicendurancemendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(classicenduranceladiesdf$Name)){
  if(classicenduranceladiesdf$Name[a] %in% names(classicenduranceladyelo) == FALSE){
    classicenduranceladyelo[[classicenduranceladiesdf$Name[a]]] <- list()
    classicenduranceladyelo[[classicenduranceladiesdf$Name[a]]][["0000"]]<-list()
    classicenduranceladyelo[[classicenduranceladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    classicenduranceladyelo[[classicenduranceladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
saveclassicendurancemaleelo<-classicendurancemaleelo
saveclassicenduranceladyelo <- classicenduranceladyelo

classicendurancemaleelo<-saveclassicendurancemaleelo

allelo <- c()
K=1
place_index = 1

for(z in 1:length(classicendurancemaleraces)){
  print(z)
  
  for(a in 1:length(classicendurancemaleraces[[z]])){
    
    
    for(b in 1:length(classicendurancemaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(classicendurancemaleraces[[z]][[a]][[b]])){
        lastp = classicendurancemaleelo[[names(classicendurancemaleraces[[z]][[a]][[b]][c])]][[length(classicendurancemaleelo[[names(classicendurancemaleraces[[z]][[a]][[b]][c])]])]][[length(classicendurancemaleelo[[names(classicendurancemaleraces[[z]][[a]][[b]][c])]][[length(classicendurancemaleelo[[names(classicendurancemaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(classicendurancemaleraces[[z]][[a]][[b]]), as.double(classicendurancemendf$Place[place_index:(place_index+length(names(classicendurancemaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(classicendurancemaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(classicendurancemaleraces[[z]][[a]][[b]][c])
        current_year <- names(classicendurancemaleraces[z])
        if(current_year %in% names(classicendurancemaleelo[[names(classicendurancemaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          classicendurancemaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(classicendurancemaleraces[[z]][a])
        if(current_date %in% names(classicendurancemaleelo[[names(classicendurancemaleraces[[z]][[a]][[b]][c])]][[names(classicendurancemaleraces[z])]])==FALSE){
          classicendurancemaleelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(classicendurancemaleraces[[z]][[a]][b])
        if(current_race %in% names(classicendurancemaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          classicendurancemaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(classicendurancemaleelo)){
    if(length(classicendurancemaleelo[[d]])>1){
      plastp = classicendurancemaleelo[[d]][[length(classicendurancemaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #classicendurancemaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      classicendurancemaleelo[[d]][[current_year]][[current_date]] <- list()
      classicendurancemaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)
classicenduranceladyelo <- saveclassicenduranceladyelo
place_index = 1
allelo <- c()
for(z in 1:length(classicenduranceladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(classicenduranceladyraces[[z]])){
    
    
    for(b in 1:length(classicenduranceladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(classicenduranceladyraces[[z]][[a]][[b]])){
        lastp = classicenduranceladyelo[[names(classicenduranceladyraces[[z]][[a]][[b]][c])]][[length(classicenduranceladyelo[[names(classicenduranceladyraces[[z]][[a]][[b]][c])]])]][[length(classicenduranceladyelo[[names(classicenduranceladyraces[[z]][[a]][[b]][c])]][[length(classicenduranceladyelo[[names(classicenduranceladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(classicenduranceladyraces[[z]][[a]][[b]]), as.double(classicenduranceladiesdf$Place[place_index:(place_index+length(names(classicenduranceladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(classicenduranceladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(classicenduranceladyraces[[z]][[a]][[b]][c])
        current_year <- names(classicenduranceladyraces[z])
        if(current_year %in% names(classicenduranceladyelo[[names(classicenduranceladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          classicenduranceladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(classicenduranceladyraces[[z]][a])
        if(current_date %in% names(classicenduranceladyelo[[names(classicenduranceladyraces[[z]][[a]][[b]][c])]][[names(classicenduranceladyraces[z])]])==FALSE){
          classicenduranceladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(classicenduranceladyraces[[z]][[a]][b])
        if(current_race %in% names(classicenduranceladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          classicenduranceladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(classicenduranceladyelo)){
    if(length(classicenduranceladyelo[[d]])>1){
      plastp = classicenduranceladyelo[[d]][[length(classicenduranceladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #classicenduranceladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      classicenduranceladyelo[[d]][[current_year]][[current_date]] <- list()
      classicenduranceladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)


classicendurancemaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

classicendurancemalenames = c()
blseason = c()
classicendurancemaledate = c()
classicendurancemaledistance = c()
classicendurancemaleeloscore = c()
for (a in 1:length(classicendurancemaleelo)){
  tick=0
  for(b in 1:length(classicendurancemaleelo[[a]])){
    for(c in 1:length(classicendurancemaleelo[[a]][[b]])){
      for(d in 1:length(classicendurancemaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #classicendurancemalenames2 = append(classicendurancemalenames2,as.character(names(classicendurancemaleelo[a])))
      }
    }
  }
  classicendurancemalenames = append(classicendurancemalenames, rep(as.character(names(classicendurancemaleelo[a])), tick))
}


for (a in 1:length(classicendurancemaleelo)){
  for(b in 1:length(classicendurancemaleelo[[a]])){
    for(c in 1:length(classicendurancemaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(classicendurancemaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      classicendurancemaledate=append(classicendurancemaledate, rep(as.character(names(classicendurancemaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(classicendurancemaleelo)){
  for(b in 1:length(classicendurancemaleelo[[a]])){
    for(c in 1:length(classicendurancemaleelo[[a]][[b]])){
      for(d in 1:length(classicendurancemaleelo[[a]][[b]][[c]])){
        classicendurancemaledistance=append(classicendurancemaledistance, as.character(names(classicendurancemaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(classicendurancemaleelo)){
  for(b in 1:length(classicendurancemaleelo[[a]])){
    for(c in 1:length(classicendurancemaleelo[[a]][[b]])){
      for(d in 1:length(classicendurancemaleelo[[a]][[b]][[c]])){
        classicendurancemaleeloscore=append(classicendurancemaleeloscore, as.double(classicendurancemaleelo[[a]][[b]][[c]][[d]]))}}}}


classicendurancemaleelodf <- data.frame(classicendurancemalenames, classicendurancemaledate, classicendurancemaledistance, classicendurancemaleeloscore)



classicenduranceladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

classicenduranceladynames = c()
blseason = c()
classicenduranceladydate = c()
classicenduranceladydistance = c()
classicenduranceladyeloscore = c()
for (a in 1:length(classicenduranceladyelo)){
  tick=0
  for(b in 1:length(classicenduranceladyelo[[a]])){
    for(c in 1:length(classicenduranceladyelo[[a]][[b]])){
      for(d in 1:length(classicenduranceladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #classicenduranceladynames2 = append(classicenduranceladynames2,as.character(names(classicenduranceladyelo[a])))
      }
    }
  }
  classicenduranceladynames = append(classicenduranceladynames, rep(as.character(names(classicenduranceladyelo[a])), tick))
}


for (a in 1:length(classicenduranceladyelo)){
  for(b in 1:length(classicenduranceladyelo[[a]])){
    for(c in 1:length(classicenduranceladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(classicenduranceladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      classicenduranceladydate=append(classicenduranceladydate, rep(as.character(names(classicenduranceladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(classicenduranceladyelo)){
  for(b in 1:length(classicenduranceladyelo[[a]])){
    for(c in 1:length(classicenduranceladyelo[[a]][[b]])){
      for(d in 1:length(classicenduranceladyelo[[a]][[b]][[c]])){
        classicenduranceladydistance=append(classicenduranceladydistance, as.character(names(classicenduranceladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(classicenduranceladyelo)){
  for(b in 1:length(classicenduranceladyelo[[a]])){
    for(c in 1:length(classicenduranceladyelo[[a]][[b]])){
      for(d in 1:length(classicenduranceladyelo[[a]][[b]][[c]])){
        classicenduranceladyeloscore=append(classicenduranceladyeloscore, as.double(classicenduranceladyelo[[a]][[b]][[c]][[d]]))}}}}


classicenduranceladyelodf <- data.frame(classicenduranceladynames, classicenduranceladydate, classicenduranceladydistance, classicenduranceladyeloscore)

saveclassicendurancemalenames <- classicendurancemalenames
saveclassicendurancemaledistance <- classicendurancemaledistance
saveclassicendurancemaledate <- classicendurancemaledate
saveclassicendurancemaleeloscore <- classicendurancemaleeloscore
saveclassicenduranceladynames <- classicenduranceladynames
saveclassicenduranceladydistance <- classicenduranceladydistance
saveclassicenduranceladydate <- classicenduranceladydate
saveclassicenduranceladyeloscore <- classicenduranceladyeloscore


classicendurancemalehighest <- classicendurancemaleelodf[order(-classicendurancemaleelodf$classicendurancemaleeloscore), ]
classicendurancemalehighest_ind <- classicendurancemalehighest[match(unique(classicendurancemalehighest$classicendurancemalenames), classicendurancemalehighest$classicendurancemalenames), ]
row.names(classicendurancemalehighest_ind) <- 1:length(classicendurancemalehighest_ind[,1])
classicendurancemalehighest_ind[1:25,]


classicenduranceladyhighest <- classicenduranceladyelodf[order(-classicenduranceladyelodf$classicenduranceladyeloscore), ]
classicenduranceladyhighest_ind <-classicenduranceladyhighest[match(unique(classicenduranceladyhighest$classicenduranceladynames), classicenduranceladyhighest$classicenduranceladynames), ]
row.names(classicenduranceladyhighest_ind) <- 1:length(classicenduranceladyhighest_ind[,1])
classicenduranceladyhighest_ind[1:25, ]

classicendurancemalelast_race <- classicendurancemaleelodf[classicendurancemaleelodf$classicendurancemaledate=="20200500",]
classicendurancemalelast_race[order(-classicendurancemalelast_race$classicendurancemaleeloscore),]

library(ggplot2)

jhk = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(classicendurancemaleeloscore), colour=classicendurancemalenames)) +geom_point()


bd = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = classicendurancemaleelodf[(classicendurancemaleelodf$classicendurancemalenames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(classicendurancemaleeloscore), colour=classicendurancemalenames)) +geom_point()


classicendurancemaleelodf[which(classicendurancemaleelodf$classicendurancemalenames=="Kevin Brochman"),]

jd = classicenduranceladyelodf[(classicenduranceladyelodf$classicenduranceladynames)=="Jessica Diggins", ]
kr = classicenduranceladyelodf[(classicenduranceladyelodf$classicenduranceladynames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(classicenduranceladyeloscore), colour=classicenduranceladynames)) +geom_point()

tj = classicenduranceladyelodf[(classicenduranceladyelodf$classicenduranceladynames)=="Therese Johaug", ]
mb = classicenduranceladyelodf[(classicenduranceladyelodf$classicenduranceladynames)=="Marit Bjørgen", ]
io = classicenduranceladyelodf[(classicenduranceladyelodf$classicenduranceladynames)=="Ingvild Flugstad Østberg", ]
hw = classicenduranceladyelodf[(classicenduranceladyelodf$classicenduranceladynames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(classicenduranceladyeloscore), colour=classicenduranceladynames)) +geom_point()


classicenduranceladyelodf[(classicenduranceladyelodf$classicenduranceladynames)=="Rosie Brennan", ]


classicendurancemalelast_race <- classicendurancemaleelodf[classicendurancemaleelodf$classicendurancemaledate=="20200500",]
classicendurancemalelast_race <- classicendurancemalelast_race[order(-classicendurancemalelast_race$classicendurancemaleeloscore),]
row.names(classicendurancemalelast_race) <- 1:length(classicendurancemalelast_race[,1])
classicendurancemalelast_race[1:25,]

classicenduranceladylast_race <- classicenduranceladyelodf[classicenduranceladyelodf$classicenduranceladydate=="20200500", ]
classicenduranceladylast_race <- classicenduranceladylast_race[order(-classicenduranceladylast_race$classicenduranceladyeloscore),]
row.names(classicenduranceladylast_race) <- 1:length(classicenduranceladylast_race[,1])
classicenduranceladylast_race[1:25,]


actclassicendurancemaleelodfnames <- unique(classicendurancemaleelodf$classicendurancemalenames)
for(a in 1:length(actclassicendurancemaleelodfnames)){
  actclassicendurancemaleskier <- classicendurancemaleelodf[classicendurancemaleelodf$classicendurancemalenames==actclassicendurancemaleelodfnames[a], ]
  #print(actclassicendurancemaleskier)
  row.names(actclassicendurancemaleskier) <- 1:length(actclassicendurancemaleskier[,1])
  
  #Starting from the back, the last classicendurancemaledistance that is not zero.  Then that one plus one is 
  actclassicendurancemaleraces <- as.numeric(as.character(row.names(actclassicendurancemaleskier[which(actclassicendurancemaleskier$classicendurancemaledistance!="0"), ])))
  actclassicendurancemalelast <- actclassicendurancemaleskier[1:actclassicendurancemaleraces[length(actclassicendurancemaleraces)]+1, ]
  
  if(a==1){
    row.names(actclassicendurancemalelast) <- 1:length(actclassicendurancemalelast[,1])
    actclassicendurancemaleelodf<-actclassicendurancemalelast
  }
  else{
    #print(length(actclassicendurancemaleelodfnames)-a)
    row.names(actclassicendurancemalelast) <- (length(actclassicendurancemaleelodf[,1])+1):(length(actclassicendurancemaleelodf[,1])+length(actclassicendurancemalelast[,1]))
    actclassicendurancemaleelodf <- rbind(actclassicendurancemaleelodf, actclassicendurancemalelast)
  }
}


actclassicenduranceladyelodfnames <- unique(classicenduranceladyelodf$classicenduranceladynames)
for(a in 1:length(actclassicenduranceladyelodfnames)){
  actclassicenduranceladyskier <- classicenduranceladyelodf[classicenduranceladyelodf$classicenduranceladynames==actclassicenduranceladyelodfnames[a], ]
  #print(actclassicenduranceladyskier)
  row.names(actclassicenduranceladyskier) <- 1:length(actclassicenduranceladyskier[,1])
  
  #Starting from the back, the last classicenduranceladydistance that is not zero.  Then that one plus one is 
  actclassicenduranceladyraces <- as.numeric(as.character(row.names(actclassicenduranceladyskier[which(actclassicenduranceladyskier$classicenduranceladydistance!="0"), ])))
  actclassicenduranceladylast <- actclassicenduranceladyskier[1:actclassicenduranceladyraces[length(actclassicenduranceladyraces)]+1, ]
  
  if(a==1){
    row.names(actclassicenduranceladylast) <- 1:length(actclassicenduranceladylast[,1])
    actclassicenduranceladyelodf<-actclassicenduranceladylast
  }
  else{
    #print(length(actclassicenduranceladyelodfnames)-a)
    row.names(actclassicenduranceladylast) <- (length(actclassicenduranceladyelodf[,1])+1):(length(actclassicenduranceladyelodf[,1])+length(actclassicenduranceladylast[,1]))
    actclassicenduranceladyelodf <- rbind(actclassicenduranceladyelodf, actclassicenduranceladylast)
  }
}
actclassicendurancemalenation <- c()
actclassicendurancemaleseason <- c()
for(a in unique(sort(actclassicendurancemaleelodf$classicendurancemaledate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actclassicendurancemaleseason <- append(actclassicendurancemaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(actclassicendurancemaleelodf[actclassicendurancemaleelodf$classicendurancemaledate==a, 1])))
  }
  else{
    actclassicendurancemaleseason <- append(actclassicendurancemaleseason, rep(as.numeric(substr(a, 1, 4)), length(actclassicendurancemaleelodf[actclassicendurancemaleelodf$classicendurancemaledate==a, 1])))#myear <- order(-myear$mclassicendurancemaleeloscore)
  }
}

for(a in unique(actclassicendurancemaleelodf$classicendurancemalenames)){
  #print(a)
  actclassicendurancemalenation <- append(actclassicendurancemalenation, rep(mendf[mendf$Name==a,]$Nationality[length(mendf[mendf$Name==a,]$Nationality)], 
                                                               length(actclassicendurancemaleelodf[actclassicendurancemaleelodf$classicendurancemalenames==a,1])))
}
actclassicendurancemaleelodf$classicendurancemaleseason <- NA
actclassicendurancemaleelodf[order(as.character(actclassicendurancemaleelodf$classicendurancemaledate)),]$classicendurancemaleseason <- actclassicendurancemaleseason
actclassicendurancemaleelodf$classicendurancemalenation <- actclassicendurancemalenation

actclassicenduranceladynation <- c()
actclassicenduranceladyseason <- c()
for(a in unique(sort(actclassicenduranceladyelodf$classicenduranceladydate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actclassicenduranceladyseason <- append(actclassicenduranceladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(actclassicenduranceladyelodf[actclassicenduranceladyelodf$classicenduranceladydate==a, 1])))
  }
  else{
    actclassicenduranceladyseason <- append(actclassicenduranceladyseason, rep(as.numeric(substr(a, 1, 4)), length(actclassicenduranceladyelodf[actclassicenduranceladyelodf$classicenduranceladydate==a, 1])))#myear <- order(-myear$mclassicendurancemaleeloscore)
  }
}

for(a in unique(actclassicenduranceladyelodf$classicenduranceladynames)){
  #print(a)
  actclassicenduranceladynation <- append(actclassicenduranceladynation, rep(classicenduranceladiesdf[classicenduranceladiesdf$Name==a,]$Nationality[length(classicenduranceladiesdf[classicenduranceladiesdf$Name==a,]$Nationality)], 
                                                               length(actclassicenduranceladyelodf[actclassicenduranceladyelodf$classicenduranceladynames==a,1])))
}
actclassicenduranceladyelodf$classicenduranceladynation <- actclassicenduranceladynation
#actclassicenduranceladyelodf <- as.data.frame(actclassicenduranceladyelodf)
actclassicenduranceladyelodf$classicenduranceladyseason <- NA
actclassicenduranceladyelodf[order(actclassicenduranceladyelodf$classicenduranceladydate),]$classicenduranceladyseason <- (actclassicenduranceladyseason)


actclassicendurancemaleelodf[actclassicendurancemaleelodf$classicendurancemalenames=="Johannes Høsflot Klæbo", ]
actclassicenduranceladyelodf[actclassicenduranceladyelodf$classicenduranceladynames=="Jessica Diggins", ]

actclassicendurancemalelast_race <- actclassicendurancemaleelodf[actclassicendurancemaleelodf$classicendurancemaledate=="20200500",]
actclassicendurancemalelast_race <- actclassicendurancemalelast_race[order(-actclassicendurancemalelast_race$classicendurancemaleeloscore),]
row.names(actclassicendurancemalelast_race) <- 1:length(actclassicendurancemalelast_race[,1])
#actclassicendurancemalelast_race
actclassicendurancemalelast_race[1:25,]
actclassicendurancemalelast_race[actclassicendurancemalelast_race$classicendurancemalenames=="Dario Cologna", ]

actclassicenduranceladylast_race <- actclassicenduranceladyelodf[actclassicenduranceladyelodf$classicenduranceladydate=="20200500",]
actclassicenduranceladylast_race <- actclassicenduranceladylast_race[order(-actclassicenduranceladylast_race$classicenduranceladyeloscore),]
row.names(actclassicenduranceladylast_race) <- 1:length(actclassicenduranceladylast_race[,1])
actclassicenduranceladylast_race[1:25, ]
actclassicenduranceladylast_race[actclassicenduranceladylast_race$classicenduranceladynames=="Jessica Diggins", ]


classicendurancemaleseasonstandings <- actclassicendurancemaleelodf[which(endsWith(as.character(actclassicendurancemaleelodf$classicendurancemaledate), "0500") ), ]
classicendurancemaleseasonstandings <- classicendurancemaleseasonstandings[order((as.character(classicendurancemaleseasonstandings$classicendurancemaledate))), ]
classicendurancemalerank <- c()

for(a in unique(classicendurancemaleseasonstandings$classicendurancemaledate)){
  classicendurancemaleseasonstandings[classicendurancemaleseasonstandings$classicendurancemaledate==a, ] <- classicendurancemaleseasonstandings[classicendurancemaleseasonstandings$classicendurancemaledate==a, ][order(-classicendurancemaleseasonstandings[classicendurancemaleseasonstandings$classicendurancemaledate==a, ]$classicendurancemaleeloscore), ]
  classicendurancemalerank <- append(classicendurancemalerank, 1:length(classicendurancemaleseasonstandings[classicendurancemaleseasonstandings$classicendurancemaledate==a, 1]))
}
classicendurancemaleseasonstandings$classicendurancemalerank <- classicendurancemalerank
row.names(classicendurancemaleseasonstandings)<-1:length(classicendurancemaleseasonstandings[,1])
mwrong_names <- c()
# for(a in unique(classicendurancemaleseasonstandings$classicendurancemalenames)){
#   #classicendurancemaleseasonstandings[classicendurancemaleseasonstandings$classicendurancemalenames==a,] <- mendf
#   temp_nation <- mendf[mendf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     mwrong_names <- append(mwrong_names, a)
#   }
#   #if(length(classicendurancemaleseasonstandings[classicendurancemaleseasonstandings$classicendurancemalenames==a,1])>15){
#   #  print(a)
#   #}
#   classicendurancemaleseason_diff <- mendf[mendf$Name==a,]
#   #classicendurancemaleseasonnation <- append(classicendurancemaleseasonnation, mendf[mendf$Name==a,])
# }
# mwrong_names <- mwrong_names[! mwrong_names %in% c("Mikhail Devjatiarov", "Vladimir Smirnov", "Alexandr Uschkalenko", "Alexei Prokourorov", 
#                                 "Igor Badamchin", "Aleksandr Golubev", "Urmas Välbe", "Andrej Kirillov", "German Karatschewskij",
#                                 "Viatscheslav Plaksunov", "Gennadiy Lasutin", "Igor Obukhov", "Elmo Kassin", "Alexander Karatschewskij")]


classicendurancemaleseasonstandings[classicendurancemaleseasonstandings$classicendurancemaledate=="20200500", ]
# View(classicendurancemaleseasonstandings[classicendurancemaleseasonstandings$classicendurancemalenames=="Gunde Svan", ])
# View(mendf[mendf$Name=="Gunde Svan", ])
# View(classicendurancemaleseasonstandings[classicendurancemaleseasonstandings$classicendurancemalenation=="Norway",])
# View(classicendurancemaleseasonstandings)

classicenduranceladyseasonstandings <- actclassicenduranceladyelodf[which(endsWith(as.character(actclassicenduranceladyelodf$classicenduranceladydate), "0500") ), ]
classicenduranceladyseasonstandings <- classicenduranceladyseasonstandings[order((as.character(classicenduranceladyseasonstandings$classicenduranceladydate))), ]
classicenduranceladyrank <- c()
for(a in unique(classicenduranceladyseasonstandings$classicenduranceladydate)){
  classicenduranceladyseasonstandings[classicenduranceladyseasonstandings$classicenduranceladydate==a, ] <- 
    classicenduranceladyseasonstandings[classicenduranceladyseasonstandings$classicenduranceladydate==a, ][order(-classicenduranceladyseasonstandings[classicenduranceladyseasonstandings$classicenduranceladydate==a, ]$classicenduranceladyeloscore), ]
  #lyear <- order(-lyear$lclassicendurancemaleeloscore)
  classicenduranceladyrank <- append(classicenduranceladyrank, 1:length(classicenduranceladyseasonstandings[classicenduranceladyseasonstandings$classicenduranceladydate==a, 1]))
}
classicenduranceladyseasonstandings$classicenduranceladyrank <- classicenduranceladyrank
row.names(classicenduranceladyseasonstandings)<-1:length(classicenduranceladyseasonstandings[,1])


lwrong_names <- c()
# for(a in unique(classicenduranceladyseasonstandings$classicenduranceladynames)){
#   #classicendurancemaleseasonstandings[classicendurancemaleseasonstandings$classicendurancemalenames==a,] <- classicenduranceladiesdf
#   temp_nation <- classicenduranceladiesdf[classicenduranceladiesdf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     lwrong_names <- append(lwrong_names, a)
#   }
#  # if(length(classicenduranceladyseasonstandings[classicenduranceladyseasonstandings$classicenduranceladynames==a,1])>20){
#   #  print(a)
#  # }
#   classicenduranceladyseason_diff <- classicenduranceladiesdf[classicenduranceladiesdf$Name==a,]
#   #classicendurancemaleseasonnation <- append(classicendurancemaleseasonnation, classicenduranceladiesdf[classicenduranceladiesdf$Name==a,])
# }


classicenduranceladyseasonstandings[classicenduranceladyseasonstandings$classicenduranceladydate=="20200500", ]
classicenduranceladyseasonstandings[classicenduranceladyseasonstandings$classicenduranceladynames=="Jessica Diggins", ]
View(classicenduranceladiesdf[classicenduranceladiesdf$Name=="Lilia Vasilieva", ])
View(classicenduranceladyseasonstandings[classicenduranceladyseasonstandings$classicenduranceladynation=="USA",])
View(classicenduranceladyseasonstandings)

