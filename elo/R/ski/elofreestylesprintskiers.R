library(readxl)
freestylesprintmen <- read_excel("~/ski/elo/excel365/all.xlsx", 
                               sheet = "Men", col_names = FALSE, na = "NA")

freestylesprintladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                                  sheet = "Ladies", col_names = FALSE, na = "NA")

#freestylesprintladiesdf[4721, 9] <- "Tatjana Kuznetsova2"
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
freestylesprintmendf <- data.frame(freestylesprintmen)
freestylesprintmendf[2073,9] <- "Oddmund Jensen2"
names(freestylesprintmendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
freestylesprintmendf$Seasons = NA

malefreestylesprintdistances <-c("Sprint") 
malefreestylesprintdiscipline <- c("F")
freestylesprintmendf <- freestylesprintmendf[as.character(freestylesprintmendf$Distance) %in% malefreestylesprintdistances, ]
freestylesprintmendf <- freestylesprintmendf[as.character(freestylesprintmendf$Discipline) %in% malefreestylesprintdiscipline, ]
row.names(freestylesprintmendf) <- 1:length(freestylesprintmendf[,1])
freestylesprintladiesdf <- data.frame(freestylesprintladies)
freestylesprintladiesdf[24331, 9] <- "Tatjana Kuznetsova2"
names(freestylesprintladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
freestylesprintladiesdf$Seasons = NA

ladyfreestylesprintdistances <- c("Sprint")
ladyfreestylesprintdiscipline <- c("F")
freestylesprintladiesdf <- freestylesprintladiesdf[as.character(freestylesprintladiesdf$Discipline) %in% ladyfreestylesprintdiscipline, ]

freestylesprintladiesdf <- freestylesprintladiesdf[as.character(freestylesprintladiesdf$Distance) %in% ladyfreestylesprintdistances, ]
row.names(freestylesprintladiesdf) <- 1:length(freestylesprintladiesdf[,1])


for(a in 1:length(freestylesprintmendf$Date)){
  if((as.double(substr(freestylesprintmendf$Date[a], 5, 8)) > 1000) && as.double(substr(freestylesprintmendf$Date[a],5,8)!=9999)){
    freestylesprintmendf$Seasons[a] = as.character(as.double(substr(freestylesprintmendf$Date[a], 1, 4))+1)
  }
  else{
    freestylesprintmendf$Seasons[a] = as.character(as.double(substr(freestylesprintmendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(freestylesprintladiesdf$Date)){
  if((as.double(substr(freestylesprintladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(freestylesprintladiesdf$Date[a],5,8)!=9999)){
    freestylesprintladiesdf$Seasons[a] = as.character(as.double(substr(freestylesprintladiesdf$Date[a], 1, 4))+1)
  }
  else{
    freestylesprintladiesdf$Seasons[a] = as.character(as.double(substr(freestylesprintladiesdf$Date[a], 1, 4)))
  }
}

freestylesprintmaleraces = list()
freestylesprintladyraces = list()

for(a in 1:length(freestylesprintmendf$Seasons)){
  if(freestylesprintmendf$Seasons[a] %in% names(freestylesprintmaleraces)==FALSE){
    freestylesprintmaleraces[[freestylesprintmendf$Seasons[a]]] <- list()
  }
  
  if(freestylesprintmendf$Date[a] %in% names(freestylesprintmaleraces[[freestylesprintmendf$Seasons[a]]])==FALSE){
    freestylesprintmaleraces[[freestylesprintmendf$Seasons[a]]][[freestylesprintmendf$Date[a]]] <- list()
  }
  if(freestylesprintmendf$Distance[a] %in% names(freestylesprintmaleraces[[freestylesprintmendf$Seasons[a]]][[freestylesprintmendf$Date[a]]])==FALSE){
    freestylesprintmaleraces[[freestylesprintmendf$Seasons[a]]][[freestylesprintmendf$Date[a]]][[freestylesprintmendf$Distance[a]]]<-list()
  }
  if(freestylesprintmendf$Name[a] %in% names(freestylesprintmaleraces[[freestylesprintmendf$Seasons[a]]][[freestylesprintmendf$Date[a]]][[freestylesprintmendf$Distance[a]]])==FALSE){
    freestylesprintmaleraces[[freestylesprintmendf$Seasons[a]]][[freestylesprintmendf$Date[a]]][[freestylesprintmendf$Distance[a]]][[freestylesprintmendf$Name[a]]]<-as.double(freestylesprintmendf$Place[a])
  }
}

for(a in 1:length(freestylesprintladiesdf$Seasons)){
  if(freestylesprintladiesdf$Seasons[a] %in% names(freestylesprintladyraces)==FALSE){
    freestylesprintladyraces[[freestylesprintladiesdf$Seasons[a]]] <- list()
  }
  
  if(freestylesprintladiesdf$Date[a] %in% names(freestylesprintladyraces[[freestylesprintladiesdf$Seasons[a]]])==FALSE){
    freestylesprintladyraces[[freestylesprintladiesdf$Seasons[a]]][[freestylesprintladiesdf$Date[a]]] <- list()
  }
  if(freestylesprintladiesdf$Distance[a] %in% names(freestylesprintladyraces[[freestylesprintladiesdf$Seasons[a]]][[freestylesprintladiesdf$Date[a]]])==FALSE){
    freestylesprintladyraces[[freestylesprintladiesdf$Seasons[a]]][[freestylesprintladiesdf$Date[a]]][[freestylesprintladiesdf$Distance[a]]]<-list()
  }
  if(freestylesprintladiesdf$Name[a] %in% names(freestylesprintladyraces[[freestylesprintladiesdf$Seasons[a]]][[freestylesprintladiesdf$Date[a]]][[freestylesprintladiesdf$Distance[a]]])==FALSE){
    freestylesprintladyraces[[freestylesprintladiesdf$Seasons[a]]][[freestylesprintladiesdf$Date[a]]][[freestylesprintladiesdf$Distance[a]]][[freestylesprintladiesdf$Name[a]]]<-as.double(freestylesprintladiesdf$Place[a])
  }
}

savefreestylesprintmaleraces<-freestylesprintmaleraces
savefreestylesprintladyraces <- freestylesprintladyraces

#Initialize all skiers to have an Elo of 1300
freestylesprintmaleelo = list()
freestylesprintladyelo = list()
for(a in 1:length(freestylesprintmendf$Name)){
  if(freestylesprintmendf$Name[a] %in% names(freestylesprintmaleelo) == FALSE){
    freestylesprintmaleelo[[freestylesprintmendf$Name[a]]] <- list()
    freestylesprintmaleelo[[freestylesprintmendf$Name[a]]][["0000"]]<-list()
    freestylesprintmaleelo[[freestylesprintmendf$Name[a]]][["0000"]][["00000000"]]<-list()
    freestylesprintmaleelo[[freestylesprintmendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(freestylesprintladiesdf$Name)){
  if(freestylesprintladiesdf$Name[a] %in% names(freestylesprintladyelo) == FALSE){
    freestylesprintladyelo[[freestylesprintladiesdf$Name[a]]] <- list()
    freestylesprintladyelo[[freestylesprintladiesdf$Name[a]]][["0000"]]<-list()
    freestylesprintladyelo[[freestylesprintladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    freestylesprintladyelo[[freestylesprintladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savefreestylesprintmaleelo<-freestylesprintmaleelo
savefreestylesprintladyelo <- freestylesprintladyelo

freestylesprintmaleelo<-savefreestylesprintmaleelo

allelo <- c()
K=1
place_index = 1

for(z in 1:length(freestylesprintmaleraces)){
  print(z)
  
  for(a in 1:length(freestylesprintmaleraces[[z]])){
    
    
    for(b in 1:length(freestylesprintmaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(freestylesprintmaleraces[[z]][[a]][[b]])){
        lastp = freestylesprintmaleelo[[names(freestylesprintmaleraces[[z]][[a]][[b]][c])]][[length(freestylesprintmaleelo[[names(freestylesprintmaleraces[[z]][[a]][[b]][c])]])]][[length(freestylesprintmaleelo[[names(freestylesprintmaleraces[[z]][[a]][[b]][c])]][[length(freestylesprintmaleelo[[names(freestylesprintmaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(freestylesprintmaleraces[[z]][[a]][[b]]), as.double(freestylesprintmendf$Place[place_index:(place_index+length(names(freestylesprintmaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(freestylesprintmaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(freestylesprintmaleraces[[z]][[a]][[b]][c])
        current_year <- names(freestylesprintmaleraces[z])
        if(current_year %in% names(freestylesprintmaleelo[[names(freestylesprintmaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          freestylesprintmaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(freestylesprintmaleraces[[z]][a])
        if(current_date %in% names(freestylesprintmaleelo[[names(freestylesprintmaleraces[[z]][[a]][[b]][c])]][[names(freestylesprintmaleraces[z])]])==FALSE){
          freestylesprintmaleelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(freestylesprintmaleraces[[z]][[a]][b])
        if(current_race %in% names(freestylesprintmaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          freestylesprintmaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(freestylesprintmaleelo)){
    if(length(freestylesprintmaleelo[[d]])>1){
      plastp = freestylesprintmaleelo[[d]][[length(freestylesprintmaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #freestylesprintmaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      freestylesprintmaleelo[[d]][[current_year]][[current_date]] <- list()
      freestylesprintmaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)
freestylesprintladyelo <- savefreestylesprintladyelo
place_index = 1
allelo <- c()
for(z in 1:length(freestylesprintladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(freestylesprintladyraces[[z]])){
    
    
    for(b in 1:length(freestylesprintladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(freestylesprintladyraces[[z]][[a]][[b]])){
        lastp = freestylesprintladyelo[[names(freestylesprintladyraces[[z]][[a]][[b]][c])]][[length(freestylesprintladyelo[[names(freestylesprintladyraces[[z]][[a]][[b]][c])]])]][[length(freestylesprintladyelo[[names(freestylesprintladyraces[[z]][[a]][[b]][c])]][[length(freestylesprintladyelo[[names(freestylesprintladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(freestylesprintladyraces[[z]][[a]][[b]]), as.double(freestylesprintladiesdf$Place[place_index:(place_index+length(names(freestylesprintladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(freestylesprintladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(freestylesprintladyraces[[z]][[a]][[b]][c])
        current_year <- names(freestylesprintladyraces[z])
        if(current_year %in% names(freestylesprintladyelo[[names(freestylesprintladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          freestylesprintladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(freestylesprintladyraces[[z]][a])
        if(current_date %in% names(freestylesprintladyelo[[names(freestylesprintladyraces[[z]][[a]][[b]][c])]][[names(freestylesprintladyraces[z])]])==FALSE){
          freestylesprintladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(freestylesprintladyraces[[z]][[a]][b])
        if(current_race %in% names(freestylesprintladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          freestylesprintladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(freestylesprintladyelo)){
    if(length(freestylesprintladyelo[[d]])>1){
      plastp = freestylesprintladyelo[[d]][[length(freestylesprintladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #freestylesprintladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      freestylesprintladyelo[[d]][[current_year]][[current_date]] <- list()
      freestylesprintladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)

freestylesprintmaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

freestylesprintmalenames = c()
blseason = c()
freestylesprintmaledate = c()
freestylesprintmaledistance = c()
freestylesprintmaleeloscore = c()
for (a in 1:length(freestylesprintmaleelo)){
  tick=0
  for(b in 1:length(freestylesprintmaleelo[[a]])){
    for(c in 1:length(freestylesprintmaleelo[[a]][[b]])){
      for(d in 1:length(freestylesprintmaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #freestylesprintmalenames2 = append(freestylesprintmalenames2,as.character(names(freestylesprintmaleelo[a])))
      }
    }
  }
  freestylesprintmalenames = append(freestylesprintmalenames, rep(as.character(names(freestylesprintmaleelo[a])), tick))
}


for (a in 1:length(freestylesprintmaleelo)){
  for(b in 1:length(freestylesprintmaleelo[[a]])){
    for(c in 1:length(freestylesprintmaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(freestylesprintmaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      freestylesprintmaledate=append(freestylesprintmaledate, rep(as.character(names(freestylesprintmaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(freestylesprintmaleelo)){
  for(b in 1:length(freestylesprintmaleelo[[a]])){
    for(c in 1:length(freestylesprintmaleelo[[a]][[b]])){
      for(d in 1:length(freestylesprintmaleelo[[a]][[b]][[c]])){
        freestylesprintmaledistance=append(freestylesprintmaledistance, as.character(names(freestylesprintmaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(freestylesprintmaleelo)){
  for(b in 1:length(freestylesprintmaleelo[[a]])){
    for(c in 1:length(freestylesprintmaleelo[[a]][[b]])){
      for(d in 1:length(freestylesprintmaleelo[[a]][[b]][[c]])){
        freestylesprintmaleeloscore=append(freestylesprintmaleeloscore, as.double(freestylesprintmaleelo[[a]][[b]][[c]][[d]]))}}}}


freestylesprintmaleelodf <- data.frame(freestylesprintmalenames, freestylesprintmaledate, freestylesprintmaledistance, freestylesprintmaleeloscore)



freestylesprintladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

freestylesprintladynames = c()
blseason = c()
freestylesprintladydate = c()
freestylesprintladydistance = c()
freestylesprintladyeloscore = c()
for (a in 1:length(freestylesprintladyelo)){
  tick=0
  for(b in 1:length(freestylesprintladyelo[[a]])){
    for(c in 1:length(freestylesprintladyelo[[a]][[b]])){
      for(d in 1:length(freestylesprintladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #freestylesprintladynames2 = append(freestylesprintladynames2,as.character(names(freestylesprintladyelo[a])))
      }
    }
  }
  freestylesprintladynames = append(freestylesprintladynames, rep(as.character(names(freestylesprintladyelo[a])), tick))
}


for (a in 1:length(freestylesprintladyelo)){
  for(b in 1:length(freestylesprintladyelo[[a]])){
    for(c in 1:length(freestylesprintladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(freestylesprintladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      freestylesprintladydate=append(freestylesprintladydate, rep(as.character(names(freestylesprintladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(freestylesprintladyelo)){
  for(b in 1:length(freestylesprintladyelo[[a]])){
    for(c in 1:length(freestylesprintladyelo[[a]][[b]])){
      for(d in 1:length(freestylesprintladyelo[[a]][[b]][[c]])){
        freestylesprintladydistance=append(freestylesprintladydistance, as.character(names(freestylesprintladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(freestylesprintladyelo)){
  for(b in 1:length(freestylesprintladyelo[[a]])){
    for(c in 1:length(freestylesprintladyelo[[a]][[b]])){
      for(d in 1:length(freestylesprintladyelo[[a]][[b]][[c]])){
        freestylesprintladyeloscore=append(freestylesprintladyeloscore, as.double(freestylesprintladyelo[[a]][[b]][[c]][[d]]))}}}}


freestylesprintladyelodf <- data.frame(freestylesprintladynames, freestylesprintladydate, freestylesprintladydistance, freestylesprintladyeloscore)

savefreestylesprintmalenames <- freestylesprintmalenames
savefreestylesprintmaledistance <- freestylesprintmaledistance
savefreestylesprintmaledate <- freestylesprintmaledate
savefreestylesprintmaleeloscore <- freestylesprintmaleeloscore
savefreestylesprintladynames <- freestylesprintladynames
savefreestylesprintladydistance <- freestylesprintladydistance
savefreestylesprintladydate <- freestylesprintladydate
savefreestylesprintladyeloscore <- freestylesprintladyeloscore


freestylesprintmalehighest <- freestylesprintmaleelodf[order(-freestylesprintmaleelodf$freestylesprintmaleeloscore), ]
freestylesprintmalehighest_ind <- freestylesprintmalehighest[match(unique(freestylesprintmalehighest$freestylesprintmalenames), freestylesprintmalehighest$freestylesprintmalenames), ]
row.names(freestylesprintmalehighest_ind) <- 1:length(freestylesprintmalehighest_ind[,1])
freestylesprintmalehighest_ind[1:25,]


freestylesprintladyhighest <- freestylesprintladyelodf[order(-freestylesprintladyelodf$freestylesprintladyeloscore), ]
freestylesprintladyhighest_ind <-freestylesprintladyhighest[match(unique(freestylesprintladyhighest$freestylesprintladynames), freestylesprintladyhighest$freestylesprintladynames), ]
row.names(freestylesprintladyhighest_ind) <- 1:length(freestylesprintladyhighest_ind[,1])
freestylesprintladyhighest_ind[1:25, ]

freestylesprintmalelast_race <- freestylesprintmaleelodf[freestylesprintmaleelodf$freestylesprintmaledate=="20200500",]
freestylesprintmalelast_race[order(-freestylesprintmalelast_race$freestylesprintmaleeloscore),]

library(ggplot2)

jhk = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(freestylesprintmaleeloscore), colour=freestylesprintmalenames)) +geom_point()


bd = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = freestylesprintmaleelodf[(freestylesprintmaleelodf$freestylesprintmalenames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(freestylesprintmaleeloscore), colour=freestylesprintmalenames)) +geom_point()


freestylesprintmaleelodf[which(freestylesprintmaleelodf$freestylesprintmalenames=="Kevin Brochman"),]

jd = freestylesprintladyelodf[(freestylesprintladyelodf$freestylesprintladynames)=="Jessica Diggins", ]
kr = freestylesprintladyelodf[(freestylesprintladyelodf$freestylesprintladynames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(freestylesprintladyeloscore), colour=freestylesprintladynames)) +geom_point()

tj = freestylesprintladyelodf[(freestylesprintladyelodf$freestylesprintladynames)=="Therese Johaug", ]
mb = freestylesprintladyelodf[(freestylesprintladyelodf$freestylesprintladynames)=="Marit Bjørgen", ]
io = freestylesprintladyelodf[(freestylesprintladyelodf$freestylesprintladynames)=="Ingvild Flugstad Østberg", ]
hw = freestylesprintladyelodf[(freestylesprintladyelodf$freestylesprintladynames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(freestylesprintladyeloscore), colour=freestylesprintladynames)) +geom_point()


freestylesprintladyelodf[(freestylesprintladyelodf$freestylesprintladynames)=="Rosie Brennan", ]


freestylesprintmalelast_race <- freestylesprintmaleelodf[freestylesprintmaleelodf$freestylesprintmaledate=="20200500",]
freestylesprintmalelast_race <- freestylesprintmalelast_race[order(-freestylesprintmalelast_race$freestylesprintmaleeloscore),]
row.names(freestylesprintmalelast_race) <- 1:length(freestylesprintmalelast_race[,1])
freestylesprintmalelast_race[1:25,]

freestylesprintladylast_race <- freestylesprintladyelodf[freestylesprintladyelodf$freestylesprintladydate=="20200500", ]
freestylesprintladylast_race <- freestylesprintladylast_race[order(-freestylesprintladylast_race$freestylesprintladyeloscore),]
row.names(freestylesprintladylast_race) <- 1:length(freestylesprintladylast_race[,1])
freestylesprintladylast_race[1:25,]

actfreestylesprintmaleelodfnames <- unique(freestylesprintmaleelodf$freestylesprintmalenames)
for(a in 1:length(actfreestylesprintmaleelodfnames)){
  actfreestylesprintmaleskier <- freestylesprintmaleelodf[freestylesprintmaleelodf$freestylesprintmalenames==actfreestylesprintmaleelodfnames[a], ]
  #print(actfreestylesprintmaleskier)
  row.names(actfreestylesprintmaleskier) <- 1:length(actfreestylesprintmaleskier[,1])
  
  #Starting from the back, the last freestylesprintmaledistance that is not zero.  Then that one plus one is 
  actfreestylesprintmaleraces <- as.numeric(as.character(row.names(actfreestylesprintmaleskier[which(actfreestylesprintmaleskier$freestylesprintmaledistance!="0"), ])))
  actfreestylesprintmalelast <- actfreestylesprintmaleskier[1:actfreestylesprintmaleraces[length(actfreestylesprintmaleraces)]+1, ]
  
  if(a==1){
    row.names(actfreestylesprintmalelast) <- 1:length(actfreestylesprintmalelast[,1])
    actfreestylesprintmaleelodf<-actfreestylesprintmalelast
  }
  else{
    #print(length(actfreestylesprintmaleelodfnames)-a)
    row.names(actfreestylesprintmalelast) <- (length(actfreestylesprintmaleelodf[,1])+1):(length(actfreestylesprintmaleelodf[,1])+length(actfreestylesprintmalelast[,1]))
    actfreestylesprintmaleelodf <- rbind(actfreestylesprintmaleelodf, actfreestylesprintmalelast)
  }
}


actfreestylesprintladyelodfnames <- unique(freestylesprintladyelodf$freestylesprintladynames)
for(a in 1:length(actfreestylesprintladyelodfnames)){
  actfreestylesprintladyskier <- freestylesprintladyelodf[freestylesprintladyelodf$freestylesprintladynames==actfreestylesprintladyelodfnames[a], ]
  #print(actfreestylesprintladyskier)
  row.names(actfreestylesprintladyskier) <- 1:length(actfreestylesprintladyskier[,1])
  
  #Starting from the back, the last freestylesprintladydistance that is not zero.  Then that one plus one is 
  actfreestylesprintladyraces <- as.numeric(as.character(row.names(actfreestylesprintladyskier[which(actfreestylesprintladyskier$freestylesprintladydistance!="0"), ])))
  actfreestylesprintladylast <- actfreestylesprintladyskier[1:actfreestylesprintladyraces[length(actfreestylesprintladyraces)]+1, ]
  
  if(a==1){
    row.names(actfreestylesprintladylast) <- 1:length(actfreestylesprintladylast[,1])
    actfreestylesprintladyelodf<-actfreestylesprintladylast
  }
  else{
    #print(length(actfreestylesprintladyelodfnames)-a)
    row.names(actfreestylesprintladylast) <- (length(actfreestylesprintladyelodf[,1])+1):(length(actfreestylesprintladyelodf[,1])+length(actfreestylesprintladylast[,1]))
    actfreestylesprintladyelodf <- rbind(actfreestylesprintladyelodf, actfreestylesprintladylast)
  }
}
actfreestylesprintmalenation <- c()
actfreestylesprintmaleseason <- c()
for(a in unique(sort(actfreestylesprintmaleelodf$freestylesprintmaledate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actfreestylesprintmaleseason <- append(actfreestylesprintmaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(actfreestylesprintmaleelodf[actfreestylesprintmaleelodf$freestylesprintmaledate==a, 1])))
  }
  else{
    actfreestylesprintmaleseason <- append(actfreestylesprintmaleseason, rep(as.numeric(substr(a, 1, 4)), length(actfreestylesprintmaleelodf[actfreestylesprintmaleelodf$freestylesprintmaledate==a, 1])))#myear <- order(-myear$mfreestylesprintmaleeloscore)
  }
}

for(a in unique(actfreestylesprintmaleelodf$freestylesprintmalenames)){
  #print(a)
  actfreestylesprintmalenation <- append(actfreestylesprintmalenation, rep(mendf[mendf$Name==a,]$Nationality[length(mendf[mendf$Name==a,]$Nationality)], 
                                                                       length(actfreestylesprintmaleelodf[actfreestylesprintmaleelodf$freestylesprintmalenames==a,1])))
}
actfreestylesprintmaleelodf$freestylesprintmaleseason <- NA
actfreestylesprintmaleelodf[order(as.character(actfreestylesprintmaleelodf$freestylesprintmaledate)),]$freestylesprintmaleseason <- actfreestylesprintmaleseason
actfreestylesprintmaleelodf$freestylesprintmalenation <- actfreestylesprintmalenation

actfreestylesprintladynation <- c()
actfreestylesprintladyseason <- c()
for(a in unique(sort(actfreestylesprintladyelodf$freestylesprintladydate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actfreestylesprintladyseason <- append(actfreestylesprintladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(actfreestylesprintladyelodf[actfreestylesprintladyelodf$freestylesprintladydate==a, 1])))
  }
  else{
    actfreestylesprintladyseason <- append(actfreestylesprintladyseason, rep(as.numeric(substr(a, 1, 4)), length(actfreestylesprintladyelodf[actfreestylesprintladyelodf$freestylesprintladydate==a, 1])))#myear <- order(-myear$mfreestylesprintmaleeloscore)
  }
}

for(a in unique(actfreestylesprintladyelodf$freestylesprintladynames)){
  #print(a)
  actfreestylesprintladynation <- append(actfreestylesprintladynation, rep(freestylesprintladiesdf[freestylesprintladiesdf$Name==a,]$Nationality[length(freestylesprintladiesdf[freestylesprintladiesdf$Name==a,]$Nationality)], 
                                                                       length(actfreestylesprintladyelodf[actfreestylesprintladyelodf$freestylesprintladynames==a,1])))
}
actfreestylesprintladyelodf$freestylesprintladynation <- actfreestylesprintladynation
#actfreestylesprintladyelodf <- as.data.frame(actfreestylesprintladyelodf)
actfreestylesprintladyelodf$freestylesprintladyseason <- NA
actfreestylesprintladyelodf[order(actfreestylesprintladyelodf$freestylesprintladydate),]$freestylesprintladyseason <- (actfreestylesprintladyseason)


actfreestylesprintmaleelodf[actfreestylesprintmaleelodf$freestylesprintmalenames=="Johannes Høsflot Klæbo", ]
actfreestylesprintladyelodf[actfreestylesprintladyelodf$freestylesprintladynames=="Jessica Diggins", ]

actfreestylesprintmalelast_race <- actfreestylesprintmaleelodf[actfreestylesprintmaleelodf$freestylesprintmaledate=="20200500",]
actfreestylesprintmalelast_race <- actfreestylesprintmalelast_race[order(-actfreestylesprintmalelast_race$freestylesprintmaleeloscore),]
row.names(actfreestylesprintmalelast_race) <- 1:length(actfreestylesprintmalelast_race[,1])
#actfreestylesprintmalelast_race
actfreestylesprintmalelast_race[1:25,]
actfreestylesprintmalelast_race[actfreestylesprintmalelast_race$freestylesprintmalenames=="Dario Cologna", ]

actfreestylesprintladylast_race <- actfreestylesprintladyelodf[actfreestylesprintladyelodf$freestylesprintladydate=="20200500",]
actfreestylesprintladylast_race <- actfreestylesprintladylast_race[order(-actfreestylesprintladylast_race$freestylesprintladyeloscore),]
row.names(actfreestylesprintladylast_race) <- 1:length(actfreestylesprintladylast_race[,1])
actfreestylesprintladylast_race[1:25, ]
actfreestylesprintladylast_race[actfreestylesprintladylast_race$freestylesprintladynames=="Jessica Diggins", ]


freestylesprintmaleseasonstandings <- actfreestylesprintmaleelodf[which(endsWith(as.character(actfreestylesprintmaleelodf$freestylesprintmaledate), "0500") ), ]
freestylesprintmaleseasonstandings <- freestylesprintmaleseasonstandings[order((as.character(freestylesprintmaleseasonstandings$freestylesprintmaledate))), ]
freestylesprintmalerank <- c()

for(a in unique(freestylesprintmaleseasonstandings$freestylesprintmaledate)){
  freestylesprintmaleseasonstandings[freestylesprintmaleseasonstandings$freestylesprintmaledate==a, ] <- freestylesprintmaleseasonstandings[freestylesprintmaleseasonstandings$freestylesprintmaledate==a, ][order(-freestylesprintmaleseasonstandings[freestylesprintmaleseasonstandings$freestylesprintmaledate==a, ]$freestylesprintmaleeloscore), ]
  freestylesprintmalerank <- append(freestylesprintmalerank, 1:length(freestylesprintmaleseasonstandings[freestylesprintmaleseasonstandings$freestylesprintmaledate==a, 1]))
}
freestylesprintmaleseasonstandings$freestylesprintmalerank <- freestylesprintmalerank
row.names(freestylesprintmaleseasonstandings)<-1:length(freestylesprintmaleseasonstandings[,1])
mwrong_names <- c()
# for(a in unique(freestylesprintmaleseasonstandings$freestylesprintmalenames)){
#   #freestylesprintmaleseasonstandings[freestylesprintmaleseasonstandings$freestylesprintmalenames==a,] <- mendf
#   temp_nation <- mendf[mendf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     mwrong_names <- append(mwrong_names, a)
#   }
#   #if(length(freestylesprintmaleseasonstandings[freestylesprintmaleseasonstandings$freestylesprintmalenames==a,1])>15){
#   #  print(a)
#   #}
#   freestylesprintmaleseason_diff <- mendf[mendf$Name==a,]
#   #freestylesprintmaleseasonnation <- append(freestylesprintmaleseasonnation, mendf[mendf$Name==a,])
# }
# mwrong_names <- mwrong_names[! mwrong_names %in% c("Mikhail Devjatiarov", "Vladimir Smirnov", "Alexandr Uschkalenko", "Alexei Prokourorov", 
#                                 "Igor Badamchin", "Aleksandr Golubev", "Urmas Välbe", "Andrej Kirillov", "German Karatschewskij",
#                                 "Viatscheslav Plaksunov", "Gennadiy Lasutin", "Igor Obukhov", "Elmo Kassin", "Alexander Karatschewskij")]


freestylesprintmaleseasonstandings[freestylesprintmaleseasonstandings$freestylesprintmaledate=="20200500", ]
# View(freestylesprintmaleseasonstandings[freestylesprintmaleseasonstandings$freestylesprintmalenames=="Gunde Svan", ])
# View(mendf[mendf$Name=="Gunde Svan", ])
# View(freestylesprintmaleseasonstandings[freestylesprintmaleseasonstandings$freestylesprintmalenation=="Norway",])
# View(freestylesprintmaleseasonstandings)

freestylesprintladyseasonstandings <- actfreestylesprintladyelodf[which(endsWith(as.character(actfreestylesprintladyelodf$freestylesprintladydate), "0500") ), ]
freestylesprintladyseasonstandings <- freestylesprintladyseasonstandings[order((as.character(freestylesprintladyseasonstandings$freestylesprintladydate))), ]
freestylesprintladyrank <- c()
for(a in unique(freestylesprintladyseasonstandings$freestylesprintladydate)){
  freestylesprintladyseasonstandings[freestylesprintladyseasonstandings$freestylesprintladydate==a, ] <- 
    freestylesprintladyseasonstandings[freestylesprintladyseasonstandings$freestylesprintladydate==a, ][order(-freestylesprintladyseasonstandings[freestylesprintladyseasonstandings$freestylesprintladydate==a, ]$freestylesprintladyeloscore), ]
  #lyear <- order(-lyear$lfreestylesprintmaleeloscore)
  freestylesprintladyrank <- append(freestylesprintladyrank, 1:length(freestylesprintladyseasonstandings[freestylesprintladyseasonstandings$freestylesprintladydate==a, 1]))
}
freestylesprintladyseasonstandings$freestylesprintladyrank <- freestylesprintladyrank
row.names(freestylesprintladyseasonstandings)<-1:length(freestylesprintladyseasonstandings[,1])


lwrong_names <- c()
# for(a in unique(freestylesprintladyseasonstandings$freestylesprintladynames)){
#   #freestylesprintmaleseasonstandings[freestylesprintmaleseasonstandings$freestylesprintmalenames==a,] <- freestylesprintladiesdf
#   temp_nation <- freestylesprintladiesdf[freestylesprintladiesdf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     lwrong_names <- append(lwrong_names, a)
#   }
#  # if(length(freestylesprintladyseasonstandings[freestylesprintladyseasonstandings$freestylesprintladynames==a,1])>20){
#   #  print(a)
#  # }
#   freestylesprintladyseason_diff <- freestylesprintladiesdf[freestylesprintladiesdf$Name==a,]
#   #freestylesprintmaleseasonnation <- append(freestylesprintmaleseasonnation, freestylesprintladiesdf[freestylesprintladiesdf$Name==a,])
# }


freestylesprintladyseasonstandings[freestylesprintladyseasonstandings$freestylesprintladydate=="20200500", ]
freestylesprintladyseasonstandings[freestylesprintladyseasonstandings$freestylesprintladynames=="Jessica Diggins", ]
View(freestylesprintladiesdf[freestylesprintladiesdf$Name=="Lilia Vasilieva", ])
View(freestylesprintladyseasonstandings[freestylesprintladyseasonstandings$freestylesprintladynation=="USA",])
View(freestylesprintladyseasonstandings)


