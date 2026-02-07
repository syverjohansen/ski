library(readxl)
classicsprintmen <- read_excel("~/ski/elo/excel365/all.xlsx", 
                        sheet = "Men", col_names = FALSE, na = "NA")

classicsprintladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                                 sheet = "Ladies", col_names = FALSE, na = "NA")

#classicsprintladiesdf[4721, 9] <- "Tatjana Kuznetsova2"
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
classicsprintmendf <- data.frame(classicsprintmen)
classicsprintmendf[2073,9] <- "Oddmund Jensen2"
names(classicsprintmendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
classicsprintmendf$Seasons = NA

maleclassicsprintdistances <-c("Sprint") 
maleclassicsprintdiscipline <- c("C")
classicsprintmendf <- classicsprintmendf[as.character(classicsprintmendf$Distance) %in% maleclassicsprintdistances, ]
classicsprintmendf <- classicsprintmendf[as.character(classicsprintmendf$Discipline) %in% maleclassicsprintdiscipline, ]
row.names(classicsprintmendf) <- 1:length(classicsprintmendf[,1])
classicsprintladiesdf <- data.frame(classicsprintladies)
classicsprintladiesdf[24331, 9] <- "Tatjana Kuznetsova2"
names(classicsprintladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
classicsprintladiesdf$Seasons = NA

ladyclassicsprintdistances <- c("Sprint")
ladyclassicsprintdiscipline <- c("C")
classicsprintladiesdf <- classicsprintladiesdf[as.character(classicsprintladiesdf$Discipline) %in% ladyclassicsprintdiscipline, ]

classicsprintladiesdf <- classicsprintladiesdf[as.character(classicsprintladiesdf$Distance) %in% ladyclassicsprintdistances, ]
row.names(classicsprintladiesdf) <- 1:length(classicsprintladiesdf[,1])


for(a in 1:length(classicsprintmendf$Date)){
  if((as.double(substr(classicsprintmendf$Date[a], 5, 8)) > 1000) && as.double(substr(classicsprintmendf$Date[a],5,8)!=9999)){
    classicsprintmendf$Seasons[a] = as.character(as.double(substr(classicsprintmendf$Date[a], 1, 4))+1)
  }
  else{
    classicsprintmendf$Seasons[a] = as.character(as.double(substr(classicsprintmendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(classicsprintladiesdf$Date)){
  if((as.double(substr(classicsprintladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(classicsprintladiesdf$Date[a],5,8)!=9999)){
    classicsprintladiesdf$Seasons[a] = as.character(as.double(substr(classicsprintladiesdf$Date[a], 1, 4))+1)
  }
  else{
    classicsprintladiesdf$Seasons[a] = as.character(as.double(substr(classicsprintladiesdf$Date[a], 1, 4)))
  }
}

classicsprintmaleraces = list()
classicsprintladyraces = list()

for(a in 1:length(classicsprintmendf$Seasons)){
  if(classicsprintmendf$Seasons[a] %in% names(classicsprintmaleraces)==FALSE){
    classicsprintmaleraces[[classicsprintmendf$Seasons[a]]] <- list()
  }
  
  if(classicsprintmendf$Date[a] %in% names(classicsprintmaleraces[[classicsprintmendf$Seasons[a]]])==FALSE){
    classicsprintmaleraces[[classicsprintmendf$Seasons[a]]][[classicsprintmendf$Date[a]]] <- list()
  }
  if(classicsprintmendf$Distance[a] %in% names(classicsprintmaleraces[[classicsprintmendf$Seasons[a]]][[classicsprintmendf$Date[a]]])==FALSE){
    classicsprintmaleraces[[classicsprintmendf$Seasons[a]]][[classicsprintmendf$Date[a]]][[classicsprintmendf$Distance[a]]]<-list()
  }
  if(classicsprintmendf$Name[a] %in% names(classicsprintmaleraces[[classicsprintmendf$Seasons[a]]][[classicsprintmendf$Date[a]]][[classicsprintmendf$Distance[a]]])==FALSE){
    classicsprintmaleraces[[classicsprintmendf$Seasons[a]]][[classicsprintmendf$Date[a]]][[classicsprintmendf$Distance[a]]][[classicsprintmendf$Name[a]]]<-as.double(classicsprintmendf$Place[a])
  }
}

for(a in 1:length(classicsprintladiesdf$Seasons)){
  if(classicsprintladiesdf$Seasons[a] %in% names(classicsprintladyraces)==FALSE){
    classicsprintladyraces[[classicsprintladiesdf$Seasons[a]]] <- list()
  }
  
  if(classicsprintladiesdf$Date[a] %in% names(classicsprintladyraces[[classicsprintladiesdf$Seasons[a]]])==FALSE){
    classicsprintladyraces[[classicsprintladiesdf$Seasons[a]]][[classicsprintladiesdf$Date[a]]] <- list()
  }
  if(classicsprintladiesdf$Distance[a] %in% names(classicsprintladyraces[[classicsprintladiesdf$Seasons[a]]][[classicsprintladiesdf$Date[a]]])==FALSE){
    classicsprintladyraces[[classicsprintladiesdf$Seasons[a]]][[classicsprintladiesdf$Date[a]]][[classicsprintladiesdf$Distance[a]]]<-list()
  }
  if(classicsprintladiesdf$Name[a] %in% names(classicsprintladyraces[[classicsprintladiesdf$Seasons[a]]][[classicsprintladiesdf$Date[a]]][[classicsprintladiesdf$Distance[a]]])==FALSE){
    classicsprintladyraces[[classicsprintladiesdf$Seasons[a]]][[classicsprintladiesdf$Date[a]]][[classicsprintladiesdf$Distance[a]]][[classicsprintladiesdf$Name[a]]]<-as.double(classicsprintladiesdf$Place[a])
  }
}

saveclassicsprintmaleraces<-classicsprintmaleraces
saveclassicsprintladyraces <- classicsprintladyraces

#Initialize all skiers to have an Elo of 1300
classicsprintmaleelo = list()
classicsprintladyelo = list()
for(a in 1:length(classicsprintmendf$Name)){
  if(classicsprintmendf$Name[a] %in% names(classicsprintmaleelo) == FALSE){
    classicsprintmaleelo[[classicsprintmendf$Name[a]]] <- list()
    classicsprintmaleelo[[classicsprintmendf$Name[a]]][["0000"]]<-list()
    classicsprintmaleelo[[classicsprintmendf$Name[a]]][["0000"]][["00000000"]]<-list()
    classicsprintmaleelo[[classicsprintmendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(classicsprintladiesdf$Name)){
  if(classicsprintladiesdf$Name[a] %in% names(classicsprintladyelo) == FALSE){
    classicsprintladyelo[[classicsprintladiesdf$Name[a]]] <- list()
    classicsprintladyelo[[classicsprintladiesdf$Name[a]]][["0000"]]<-list()
    classicsprintladyelo[[classicsprintladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    classicsprintladyelo[[classicsprintladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
saveclassicsprintmaleelo<-classicsprintmaleelo
saveclassicsprintladyelo <- classicsprintladyelo

classicsprintmaleelo<-saveclassicsprintmaleelo

allelo <- c()
K=1
place_index = 1

for(z in 1:length(classicsprintmaleraces)){
  print(z)
  
  for(a in 1:length(classicsprintmaleraces[[z]])){
    
    
    for(b in 1:length(classicsprintmaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(classicsprintmaleraces[[z]][[a]][[b]])){
        lastp = classicsprintmaleelo[[names(classicsprintmaleraces[[z]][[a]][[b]][c])]][[length(classicsprintmaleelo[[names(classicsprintmaleraces[[z]][[a]][[b]][c])]])]][[length(classicsprintmaleelo[[names(classicsprintmaleraces[[z]][[a]][[b]][c])]][[length(classicsprintmaleelo[[names(classicsprintmaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(classicsprintmaleraces[[z]][[a]][[b]]), as.double(classicsprintmendf$Place[place_index:(place_index+length(names(classicsprintmaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(classicsprintmaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(classicsprintmaleraces[[z]][[a]][[b]][c])
        current_year <- names(classicsprintmaleraces[z])
        if(current_year %in% names(classicsprintmaleelo[[names(classicsprintmaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          classicsprintmaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(classicsprintmaleraces[[z]][a])
        if(current_date %in% names(classicsprintmaleelo[[names(classicsprintmaleraces[[z]][[a]][[b]][c])]][[names(classicsprintmaleraces[z])]])==FALSE){
          classicsprintmaleelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(classicsprintmaleraces[[z]][[a]][b])
        if(current_race %in% names(classicsprintmaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          classicsprintmaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(classicsprintmaleelo)){
    if(length(classicsprintmaleelo[[d]])>1){
      plastp = classicsprintmaleelo[[d]][[length(classicsprintmaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #classicsprintmaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      classicsprintmaleelo[[d]][[current_year]][[current_date]] <- list()
      classicsprintmaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)
classicsprintladyelo <- saveclassicsprintladyelo
place_index = 1
allelo <- c()
for(z in 1:length(classicsprintladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(classicsprintladyraces[[z]])){
    
    
    for(b in 1:length(classicsprintladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(classicsprintladyraces[[z]][[a]][[b]])){
        lastp = classicsprintladyelo[[names(classicsprintladyraces[[z]][[a]][[b]][c])]][[length(classicsprintladyelo[[names(classicsprintladyraces[[z]][[a]][[b]][c])]])]][[length(classicsprintladyelo[[names(classicsprintladyraces[[z]][[a]][[b]][c])]][[length(classicsprintladyelo[[names(classicsprintladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(classicsprintladyraces[[z]][[a]][[b]]), as.double(classicsprintladiesdf$Place[place_index:(place_index+length(names(classicsprintladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(classicsprintladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(classicsprintladyraces[[z]][[a]][[b]][c])
        current_year <- names(classicsprintladyraces[z])
        if(current_year %in% names(classicsprintladyelo[[names(classicsprintladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          classicsprintladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(classicsprintladyraces[[z]][a])
        if(current_date %in% names(classicsprintladyelo[[names(classicsprintladyraces[[z]][[a]][[b]][c])]][[names(classicsprintladyraces[z])]])==FALSE){
          classicsprintladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(classicsprintladyraces[[z]][[a]][b])
        if(current_race %in% names(classicsprintladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          classicsprintladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(classicsprintladyelo)){
    if(length(classicsprintladyelo[[d]])>1){
      plastp = classicsprintladyelo[[d]][[length(classicsprintladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #classicsprintladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      classicsprintladyelo[[d]][[current_year]][[current_date]] <- list()
      classicsprintladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
print(race_df)

classicsprintmaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

classicsprintmalenames = c()
blseason = c()
classicsprintmaledate = c()
classicsprintmaledistance = c()
classicsprintmaleeloscore = c()
for (a in 1:length(classicsprintmaleelo)){
  tick=0
  for(b in 1:length(classicsprintmaleelo[[a]])){
    for(c in 1:length(classicsprintmaleelo[[a]][[b]])){
      for(d in 1:length(classicsprintmaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #classicsprintmalenames2 = append(classicsprintmalenames2,as.character(names(classicsprintmaleelo[a])))
      }
    }
  }
  classicsprintmalenames = append(classicsprintmalenames, rep(as.character(names(classicsprintmaleelo[a])), tick))
}


for (a in 1:length(classicsprintmaleelo)){
  for(b in 1:length(classicsprintmaleelo[[a]])){
    for(c in 1:length(classicsprintmaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(classicsprintmaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      classicsprintmaledate=append(classicsprintmaledate, rep(as.character(names(classicsprintmaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(classicsprintmaleelo)){
  for(b in 1:length(classicsprintmaleelo[[a]])){
    for(c in 1:length(classicsprintmaleelo[[a]][[b]])){
      for(d in 1:length(classicsprintmaleelo[[a]][[b]][[c]])){
        classicsprintmaledistance=append(classicsprintmaledistance, as.character(names(classicsprintmaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(classicsprintmaleelo)){
  for(b in 1:length(classicsprintmaleelo[[a]])){
    for(c in 1:length(classicsprintmaleelo[[a]][[b]])){
      for(d in 1:length(classicsprintmaleelo[[a]][[b]][[c]])){
        classicsprintmaleeloscore=append(classicsprintmaleeloscore, as.double(classicsprintmaleelo[[a]][[b]][[c]][[d]]))}}}}


classicsprintmaleelodf <- data.frame(classicsprintmalenames, classicsprintmaledate, classicsprintmaledistance, classicsprintmaleeloscore)



classicsprintladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

classicsprintladynames = c()
blseason = c()
classicsprintladydate = c()
classicsprintladydistance = c()
classicsprintladyeloscore = c()
for (a in 1:length(classicsprintladyelo)){
  tick=0
  for(b in 1:length(classicsprintladyelo[[a]])){
    for(c in 1:length(classicsprintladyelo[[a]][[b]])){
      for(d in 1:length(classicsprintladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #classicsprintladynames2 = append(classicsprintladynames2,as.character(names(classicsprintladyelo[a])))
      }
    }
  }
  classicsprintladynames = append(classicsprintladynames, rep(as.character(names(classicsprintladyelo[a])), tick))
}


for (a in 1:length(classicsprintladyelo)){
  for(b in 1:length(classicsprintladyelo[[a]])){
    for(c in 1:length(classicsprintladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(classicsprintladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      classicsprintladydate=append(classicsprintladydate, rep(as.character(names(classicsprintladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(classicsprintladyelo)){
  for(b in 1:length(classicsprintladyelo[[a]])){
    for(c in 1:length(classicsprintladyelo[[a]][[b]])){
      for(d in 1:length(classicsprintladyelo[[a]][[b]][[c]])){
        classicsprintladydistance=append(classicsprintladydistance, as.character(names(classicsprintladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(classicsprintladyelo)){
  for(b in 1:length(classicsprintladyelo[[a]])){
    for(c in 1:length(classicsprintladyelo[[a]][[b]])){
      for(d in 1:length(classicsprintladyelo[[a]][[b]][[c]])){
        classicsprintladyeloscore=append(classicsprintladyeloscore, as.double(classicsprintladyelo[[a]][[b]][[c]][[d]]))}}}}


classicsprintladyelodf <- data.frame(classicsprintladynames, classicsprintladydate, classicsprintladydistance, classicsprintladyeloscore)

saveclassicsprintmalenames <- classicsprintmalenames
saveclassicsprintmaledistance <- classicsprintmaledistance
saveclassicsprintmaledate <- classicsprintmaledate
saveclassicsprintmaleeloscore <- classicsprintmaleeloscore
saveclassicsprintladynames <- classicsprintladynames
saveclassicsprintladydistance <- classicsprintladydistance
saveclassicsprintladydate <- classicsprintladydate
saveclassicsprintladyeloscore <- classicsprintladyeloscore


classicsprintmalehighest <- classicsprintmaleelodf[order(-classicsprintmaleelodf$classicsprintmaleeloscore), ]
classicsprintmalehighest_ind <- classicsprintmalehighest[match(unique(classicsprintmalehighest$classicsprintmalenames), classicsprintmalehighest$classicsprintmalenames), ]
row.names(classicsprintmalehighest_ind) <- 1:length(classicsprintmalehighest_ind[,1])
classicsprintmalehighest_ind[1:25,]


classicsprintladyhighest <- classicsprintladyelodf[order(-classicsprintladyelodf$classicsprintladyeloscore), ]
classicsprintladyhighest_ind <-classicsprintladyhighest[match(unique(classicsprintladyhighest$classicsprintladynames), classicsprintladyhighest$classicsprintladynames), ]
row.names(classicsprintladyhighest_ind) <- 1:length(classicsprintladyhighest_ind[,1])
classicsprintladyhighest_ind[1:25, ]

classicsprintmalelast_race <- classicsprintmaleelodf[classicsprintmaleelodf$classicsprintmaledate=="20200500",]
classicsprintmalelast_race[order(-classicsprintmalelast_race$classicsprintmaleeloscore),]

library(ggplot2)

jhk = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(classicsprintmaleeloscore), colour=classicsprintmalenames)) +geom_point()


bd = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = classicsprintmaleelodf[(classicsprintmaleelodf$classicsprintmalenames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(classicsprintmaleeloscore), colour=classicsprintmalenames)) +geom_point()


classicsprintmaleelodf[which(classicsprintmaleelodf$classicsprintmalenames=="Kevin Brochman"),]

jd = classicsprintladyelodf[(classicsprintladyelodf$classicsprintladynames)=="Jessica Diggins", ]
kr = classicsprintladyelodf[(classicsprintladyelodf$classicsprintladynames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(classicsprintladyeloscore), colour=classicsprintladynames)) +geom_point()

tj = classicsprintladyelodf[(classicsprintladyelodf$classicsprintladynames)=="Therese Johaug", ]
mb = classicsprintladyelodf[(classicsprintladyelodf$classicsprintladynames)=="Marit Bjørgen", ]
io = classicsprintladyelodf[(classicsprintladyelodf$classicsprintladynames)=="Ingvild Flugstad Østberg", ]
hw = classicsprintladyelodf[(classicsprintladyelodf$classicsprintladynames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(classicsprintladyeloscore), colour=classicsprintladynames)) +geom_point()


classicsprintladyelodf[(classicsprintladyelodf$classicsprintladynames)=="Rosie Brennan", ]


classicsprintmalelast_race <- classicsprintmaleelodf[classicsprintmaleelodf$classicsprintmaledate=="20200500",]
classicsprintmalelast_race <- classicsprintmalelast_race[order(-classicsprintmalelast_race$classicsprintmaleeloscore),]
row.names(classicsprintmalelast_race) <- 1:length(classicsprintmalelast_race[,1])
classicsprintmalelast_race[1:25,]

classicsprintladylast_race <- classicsprintladyelodf[classicsprintladyelodf$classicsprintladydate=="20200500", ]
classicsprintladylast_race <- classicsprintladylast_race[order(-classicsprintladylast_race$classicsprintladyeloscore),]
row.names(classicsprintladylast_race) <- 1:length(classicsprintladylast_race[,1])
classicsprintladylast_race[1:25,]

actclassicsprintmaleelodfnames <- unique(classicsprintmaleelodf$classicsprintmalenames)
for(a in 1:length(actclassicsprintmaleelodfnames)){
  actclassicsprintmaleskier <- classicsprintmaleelodf[classicsprintmaleelodf$classicsprintmalenames==actclassicsprintmaleelodfnames[a], ]
  #print(actclassicsprintmaleskier)
  row.names(actclassicsprintmaleskier) <- 1:length(actclassicsprintmaleskier[,1])
  
  #Starting from the back, the last classicsprintmaledistance that is not zero.  Then that one plus one is 
  actclassicsprintmaleraces <- as.numeric(as.character(row.names(actclassicsprintmaleskier[which(actclassicsprintmaleskier$classicsprintmaledistance!="0"), ])))
  actclassicsprintmalelast <- actclassicsprintmaleskier[1:actclassicsprintmaleraces[length(actclassicsprintmaleraces)]+1, ]
  
  if(a==1){
    row.names(actclassicsprintmalelast) <- 1:length(actclassicsprintmalelast[,1])
    actclassicsprintmaleelodf<-actclassicsprintmalelast
  }
  else{
    #print(length(actclassicsprintmaleelodfnames)-a)
    row.names(actclassicsprintmalelast) <- (length(actclassicsprintmaleelodf[,1])+1):(length(actclassicsprintmaleelodf[,1])+length(actclassicsprintmalelast[,1]))
    actclassicsprintmaleelodf <- rbind(actclassicsprintmaleelodf, actclassicsprintmalelast)
  }
}


actclassicsprintladyelodfnames <- unique(classicsprintladyelodf$classicsprintladynames)
for(a in 1:length(actclassicsprintladyelodfnames)){
  actclassicsprintladyskier <- classicsprintladyelodf[classicsprintladyelodf$classicsprintladynames==actclassicsprintladyelodfnames[a], ]
  #print(actclassicsprintladyskier)
  row.names(actclassicsprintladyskier) <- 1:length(actclassicsprintladyskier[,1])
  
  #Starting from the back, the last classicsprintladydistance that is not zero.  Then that one plus one is 
  actclassicsprintladyraces <- as.numeric(as.character(row.names(actclassicsprintladyskier[which(actclassicsprintladyskier$classicsprintladydistance!="0"), ])))
  actclassicsprintladylast <- actclassicsprintladyskier[1:actclassicsprintladyraces[length(actclassicsprintladyraces)]+1, ]
  
  if(a==1){
    row.names(actclassicsprintladylast) <- 1:length(actclassicsprintladylast[,1])
    actclassicsprintladyelodf<-actclassicsprintladylast
  }
  else{
    #print(length(actclassicsprintladyelodfnames)-a)
    row.names(actclassicsprintladylast) <- (length(actclassicsprintladyelodf[,1])+1):(length(actclassicsprintladyelodf[,1])+length(actclassicsprintladylast[,1]))
    actclassicsprintladyelodf <- rbind(actclassicsprintladyelodf, actclassicsprintladylast)
  }
}
actclassicsprintmalenation <- c()
actclassicsprintmaleseason <- c()
for(a in unique(sort(actclassicsprintmaleelodf$classicsprintmaledate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actclassicsprintmaleseason <- append(actclassicsprintmaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(actclassicsprintmaleelodf[actclassicsprintmaleelodf$classicsprintmaledate==a, 1])))
  }
  else{
    actclassicsprintmaleseason <- append(actclassicsprintmaleseason, rep(as.numeric(substr(a, 1, 4)), length(actclassicsprintmaleelodf[actclassicsprintmaleelodf$classicsprintmaledate==a, 1])))#myear <- order(-myear$mclassicsprintmaleeloscore)
  }
}

for(a in unique(actclassicsprintmaleelodf$classicsprintmalenames)){
  #print(a)
  actclassicsprintmalenation <- append(actclassicsprintmalenation, rep(mendf[mendf$Name==a,]$Nationality[length(mendf[mendf$Name==a,]$Nationality)], 
                                                                       length(actclassicsprintmaleelodf[actclassicsprintmaleelodf$classicsprintmalenames==a,1])))
}
actclassicsprintmaleelodf$classicsprintmaleseason <- NA
actclassicsprintmaleelodf[order(as.character(actclassicsprintmaleelodf$classicsprintmaledate)),]$classicsprintmaleseason <- actclassicsprintmaleseason
actclassicsprintmaleelodf$classicsprintmalenation <- actclassicsprintmalenation

actclassicsprintladynation <- c()
actclassicsprintladyseason <- c()
for(a in unique(sort(actclassicsprintladyelodf$classicsprintladydate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actclassicsprintladyseason <- append(actclassicsprintladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(actclassicsprintladyelodf[actclassicsprintladyelodf$classicsprintladydate==a, 1])))
  }
  else{
    actclassicsprintladyseason <- append(actclassicsprintladyseason, rep(as.numeric(substr(a, 1, 4)), length(actclassicsprintladyelodf[actclassicsprintladyelodf$classicsprintladydate==a, 1])))#myear <- order(-myear$mclassicsprintmaleeloscore)
  }
}

for(a in unique(actclassicsprintladyelodf$classicsprintladynames)){
  #print(a)
  actclassicsprintladynation <- append(actclassicsprintladynation, rep(classicsprintladiesdf[classicsprintladiesdf$Name==a,]$Nationality[length(classicsprintladiesdf[classicsprintladiesdf$Name==a,]$Nationality)], 
                                                                       length(actclassicsprintladyelodf[actclassicsprintladyelodf$classicsprintladynames==a,1])))
}
actclassicsprintladyelodf$classicsprintladynation <- actclassicsprintladynation
#actclassicsprintladyelodf <- as.data.frame(actclassicsprintladyelodf)
actclassicsprintladyelodf$classicsprintladyseason <- NA
actclassicsprintladyelodf[order(actclassicsprintladyelodf$classicsprintladydate),]$classicsprintladyseason <- (actclassicsprintladyseason)


actclassicsprintmaleelodf[actclassicsprintmaleelodf$classicsprintmalenames=="Johannes Høsflot Klæbo", ]
actclassicsprintladyelodf[actclassicsprintladyelodf$classicsprintladynames=="Jessica Diggins", ]

actclassicsprintmalelast_race <- actclassicsprintmaleelodf[actclassicsprintmaleelodf$classicsprintmaledate=="20200500",]
actclassicsprintmalelast_race <- actclassicsprintmalelast_race[order(-actclassicsprintmalelast_race$classicsprintmaleeloscore),]
row.names(actclassicsprintmalelast_race) <- 1:length(actclassicsprintmalelast_race[,1])
#actclassicsprintmalelast_race
actclassicsprintmalelast_race[1:25,]
actclassicsprintmalelast_race[actclassicsprintmalelast_race$classicsprintmalenames=="Dario Cologna", ]

actclassicsprintladylast_race <- actclassicsprintladyelodf[actclassicsprintladyelodf$classicsprintladydate=="20200500",]
actclassicsprintladylast_race <- actclassicsprintladylast_race[order(-actclassicsprintladylast_race$classicsprintladyeloscore),]
row.names(actclassicsprintladylast_race) <- 1:length(actclassicsprintladylast_race[,1])
actclassicsprintladylast_race[1:25, ]
actclassicsprintladylast_race[actclassicsprintladylast_race$classicsprintladynames=="Jessica Diggins", ]


classicsprintmaleseasonstandings <- actclassicsprintmaleelodf[which(endsWith(as.character(actclassicsprintmaleelodf$classicsprintmaledate), "0500") ), ]
classicsprintmaleseasonstandings <- classicsprintmaleseasonstandings[order((as.character(classicsprintmaleseasonstandings$classicsprintmaledate))), ]
classicsprintmalerank <- c()

for(a in unique(classicsprintmaleseasonstandings$classicsprintmaledate)){
  classicsprintmaleseasonstandings[classicsprintmaleseasonstandings$classicsprintmaledate==a, ] <- classicsprintmaleseasonstandings[classicsprintmaleseasonstandings$classicsprintmaledate==a, ][order(-classicsprintmaleseasonstandings[classicsprintmaleseasonstandings$classicsprintmaledate==a, ]$classicsprintmaleeloscore), ]
  classicsprintmalerank <- append(classicsprintmalerank, 1:length(classicsprintmaleseasonstandings[classicsprintmaleseasonstandings$classicsprintmaledate==a, 1]))
}
classicsprintmaleseasonstandings$classicsprintmalerank <- classicsprintmalerank
row.names(classicsprintmaleseasonstandings)<-1:length(classicsprintmaleseasonstandings[,1])
mwrong_names <- c()
# for(a in unique(classicsprintmaleseasonstandings$classicsprintmalenames)){
#   #classicsprintmaleseasonstandings[classicsprintmaleseasonstandings$classicsprintmalenames==a,] <- mendf
#   temp_nation <- mendf[mendf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     mwrong_names <- append(mwrong_names, a)
#   }
#   #if(length(classicsprintmaleseasonstandings[classicsprintmaleseasonstandings$classicsprintmalenames==a,1])>15){
#   #  print(a)
#   #}
#   classicsprintmaleseason_diff <- mendf[mendf$Name==a,]
#   #classicsprintmaleseasonnation <- append(classicsprintmaleseasonnation, mendf[mendf$Name==a,])
# }
# mwrong_names <- mwrong_names[! mwrong_names %in% c("Mikhail Devjatiarov", "Vladimir Smirnov", "Alexandr Uschkalenko", "Alexei Prokourorov", 
#                                 "Igor Badamchin", "Aleksandr Golubev", "Urmas Välbe", "Andrej Kirillov", "German Karatschewskij",
#                                 "Viatscheslav Plaksunov", "Gennadiy Lasutin", "Igor Obukhov", "Elmo Kassin", "Alexander Karatschewskij")]


classicsprintmaleseasonstandings[classicsprintmaleseasonstandings$classicsprintmaledate=="20200500", ]
# View(classicsprintmaleseasonstandings[classicsprintmaleseasonstandings$classicsprintmalenames=="Gunde Svan", ])
# View(mendf[mendf$Name=="Gunde Svan", ])
# View(classicsprintmaleseasonstandings[classicsprintmaleseasonstandings$classicsprintmalenation=="Norway",])
# View(classicsprintmaleseasonstandings)

classicsprintladyseasonstandings <- actclassicsprintladyelodf[which(endsWith(as.character(actclassicsprintladyelodf$classicsprintladydate), "0500") ), ]
classicsprintladyseasonstandings <- classicsprintladyseasonstandings[order((as.character(classicsprintladyseasonstandings$classicsprintladydate))), ]
classicsprintladyrank <- c()
for(a in unique(classicsprintladyseasonstandings$classicsprintladydate)){
  classicsprintladyseasonstandings[classicsprintladyseasonstandings$classicsprintladydate==a, ] <- 
    classicsprintladyseasonstandings[classicsprintladyseasonstandings$classicsprintladydate==a, ][order(-classicsprintladyseasonstandings[classicsprintladyseasonstandings$classicsprintladydate==a, ]$classicsprintladyeloscore), ]
  #lyear <- order(-lyear$lclassicsprintmaleeloscore)
  classicsprintladyrank <- append(classicsprintladyrank, 1:length(classicsprintladyseasonstandings[classicsprintladyseasonstandings$classicsprintladydate==a, 1]))
}
classicsprintladyseasonstandings$classicsprintladyrank <- classicsprintladyrank
row.names(classicsprintladyseasonstandings)<-1:length(classicsprintladyseasonstandings[,1])


lwrong_names <- c()
# for(a in unique(classicsprintladyseasonstandings$classicsprintladynames)){
#   #classicsprintmaleseasonstandings[classicsprintmaleseasonstandings$classicsprintmalenames==a,] <- classicsprintladiesdf
#   temp_nation <- classicsprintladiesdf[classicsprintladiesdf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     lwrong_names <- append(lwrong_names, a)
#   }
#  # if(length(classicsprintladyseasonstandings[classicsprintladyseasonstandings$classicsprintladynames==a,1])>20){
#   #  print(a)
#  # }
#   classicsprintladyseason_diff <- classicsprintladiesdf[classicsprintladiesdf$Name==a,]
#   #classicsprintmaleseasonnation <- append(classicsprintmaleseasonnation, classicsprintladiesdf[classicsprintladiesdf$Name==a,])
# }


classicsprintladyseasonstandings[classicsprintladyseasonstandings$classicsprintladydate=="20200500", ]
classicsprintladyseasonstandings[classicsprintladyseasonstandings$classicsprintladynames=="Jessica Diggins", ]
View(classicsprintladiesdf[classicsprintladiesdf$Name=="Lilia Vasilieva", ])
View(classicsprintladyseasonstandings[classicsprintladyseasonstandings$classicsprintladynation=="USA",])
View(classicsprintladyseasonstandings)


