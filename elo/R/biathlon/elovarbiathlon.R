library(readxl)
library(ggplot2)
varbmen <- read_excel("~/ski/elo/biathlon/excel365/all.xlsx", 
                   sheet = "Men", col_names = FALSE, na = "NA")
varbladies <- read_excel("~/ski/elo/biathlon/excel365/all.xlsx", 
                      sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
varbmendf <- data.frame(varbmen)
names(varbmendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")

varbladiesdf <- data.frame(varbladies)
names(varbladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")

varbmendf[which(varbmendf$Name=="Uros Velepec"), ]
varbmendf[9757,9] = "Ricco Gross2"
varbmendf[9698,9] = "Ricco Gross2"
varbmendf[9365,9] = "Uros Velepec2"
varbmendf[9360, 9] = "Petr Garabik2"
varbmendf[9139,9] = "Alexandr Popov2"
varbmendf[392,9] = "Sten Eriksson2"



bmalevardates <- c("20190500", "20201209")
#malesprintdistances <-c("Sprint") 
#print(unique(varbmendf$Distance))
#varbmendf <- varbmendf[as.character(varbmendf$Distance) %in% bmalevardistances, ]
#varbmendf <- varbmendf[as.character(varbmendf$Discipline) %in% bmalevardiscipline,]
varbmendf <- subset(varbmendf, Date>bmalevardates[1] & Date<bmalevardates[2])
row.names(varbmendf) <- 1:length(varbmendf[,1])


#varbladiesdf <- varbladiesdf[as.character(varbladiesdf$Distance) %in% bladyvardistances, ]
#print(unique(varbladiesdf$Distance))
varbladiesdf <- subset(varbladiesdf, Date>bmalevardates[1] & Date<bmalevardates[2])
row.names(varbladiesdf) <- 1:length(varbladiesdf[,1])


varbmendf$Seasons = NA
varbladiesdf$Seasons = NA
for(a in 1:length(varbmendf$Date)){
  if((as.double(substr(varbmendf$Date[a], 5, 8)) > 1000) && as.double(substr(varbmendf$Date[a],5,8)!=9999)){
    varbmendf$Seasons[a] = as.character(as.double(substr(varbmendf$Date[a], 1, 4))+1)
  }
  else{
    varbmendf$Seasons[a] = as.character(as.double(substr(varbmendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(varbladiesdf$Date)){
  if((as.double(substr(varbladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(varbladiesdf$Date[a],5,8)!=9999)){
    varbladiesdf$Seasons[a] = as.character(as.double(substr(varbladiesdf$Date[a], 1, 4))+1)
  }
  else{
    varbladiesdf$Seasons[a] = as.character(as.double(substr(varbladiesdf$Date[a], 1, 4)))
  }
}

#varbladiesdf[24331, 9] <- "Tatjana Kuznetsova2"
varbmaleraces = list()
varbladyraces = list()

for(a in 1:length(varbmendf$Seasons)){
  if(varbmendf$Seasons[a] %in% names(varbmaleraces)==FALSE){
    varbmaleraces[[varbmendf$Seasons[a]]] <- list()
  }
  
  if(varbmendf$Date[a] %in% names(varbmaleraces[[varbmendf$Seasons[a]]])==FALSE){
    varbmaleraces[[varbmendf$Seasons[a]]][[varbmendf$Date[a]]] <- list()
  }
  if(varbmendf$Distance[a] %in% names(varbmaleraces[[varbmendf$Seasons[a]]][[varbmendf$Date[a]]])==FALSE){
    varbmaleraces[[varbmendf$Seasons[a]]][[varbmendf$Date[a]]][[varbmendf$Distance[a]]]<-list()
  }
  if(varbmendf$Name[a] %in% names(varbmaleraces[[varbmendf$Seasons[a]]][[varbmendf$Date[a]]][[varbmendf$Distance[a]]])==FALSE){
    varbmaleraces[[varbmendf$Seasons[a]]][[varbmendf$Date[a]]][[varbmendf$Distance[a]]][[varbmendf$Name[a]]]<-as.double(varbmendf$Place[a])
  }
}

for(a in 1:length(varbladiesdf$Seasons)){
  if(varbladiesdf$Seasons[a] %in% names(varbladyraces)==FALSE){
    varbladyraces[[varbladiesdf$Seasons[a]]] <- list()
  }
  
  if(varbladiesdf$Date[a] %in% names(varbladyraces[[varbladiesdf$Seasons[a]]])==FALSE){
    varbladyraces[[varbladiesdf$Seasons[a]]][[varbladiesdf$Date[a]]] <- list()
  }
  if(varbladiesdf$Distance[a] %in% names(varbladyraces[[varbladiesdf$Seasons[a]]][[varbladiesdf$Date[a]]])==FALSE){
    varbladyraces[[varbladiesdf$Seasons[a]]][[varbladiesdf$Date[a]]][[varbladiesdf$Distance[a]]]<-list()
  }
  if(varbladiesdf$Name[a] %in% names(varbladyraces[[varbladiesdf$Seasons[a]]][[varbladiesdf$Date[a]]][[varbladiesdf$Distance[a]]])==FALSE){
    varbladyraces[[varbladiesdf$Seasons[a]]][[varbladiesdf$Date[a]]][[varbladiesdf$Distance[a]]][[varbladiesdf$Name[a]]]<-as.double(varbladiesdf$Place[a])
  }
}





savevarbmaleraces<-varbmaleraces
savevarbladyraces <- varbladyraces

#Initialize all skiers to have an Elo of 1300
varbmalebelo = list()
varbladybelo = list()
for(a in 1:length(varbmendf$Name)){
  if(varbmendf$Name[a] %in% names(varbmalebelo) == FALSE){
    varbmalebelo[[varbmendf$Name[a]]] <- list()
    varbmalebelo[[varbmendf$Name[a]]][["0000"]]<-list()
    varbmalebelo[[varbmendf$Name[a]]][["0000"]][["00000000"]]<-list()
    varbmalebelo[[varbmendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(varbladiesdf$Name)){
  if(varbladiesdf$Name[a] %in% names(varbladybelo) == FALSE){
    varbladybelo[[varbladiesdf$Name[a]]] <- list()
    varbladybelo[[varbladiesdf$Name[a]]][["0000"]]<-list()
    varbladybelo[[varbladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    varbladybelo[[varbladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savevarbmalebelo<-varbmalebelo
savevarbladybelo <- varbladybelo

#first attempt at an belo rating
varbmalebelo<-savevarbmalebelo

allbelo <- c()
K=1
place_index = 1

for(z in 1:length(varbmaleraces)){
  #for(z in 1:35){
  # print(z)
  
  for(a in 1:length(varbmaleraces[[z]])){
    
    
    for(b in 1:length(varbmaleraces[[z]][[a]])){
      pbelo = c()
      belo = c()
      
      for(c in 1:length(varbmaleraces[[z]][[a]][[b]])){
        lastp = varbmalebelo[[names(varbmaleraces[[z]][[a]][[b]][c])]][[length(varbmalebelo[[names(varbmaleraces[[z]][[a]][[b]][c])]])]][[length(varbmalebelo[[names(varbmaleraces[[z]][[a]][[b]][c])]][[length(varbmalebelo[[names(varbmaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pbelo = append(pbelo, lastpp)                   
      }
      race_df <- data.frame(names(varbmaleraces[[z]][[a]][[b]]), as.double(varbmendf$Place[place_index:(place_index+length(names(varbmaleraces[[z]][[a]][[b]]))-1)]), pbelo)
      place_index = place_index+ length(names(varbmaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pbelo")
      
      
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pbelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(varbmaleraces[[z]][[a]][[b]][c])
        current_year <- names(varbmaleraces[z])
        if(current_year %in% names(varbmalebelo[[names(varbmaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          varbmalebelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(varbmaleraces[[z]][a])
        if(current_date %in% names(varbmalebelo[[names(varbmaleraces[[z]][[a]][[b]][c])]][[names(varbmaleraces[z])]])==FALSE){
          varbmalebelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(varbmaleraces[[z]][[a]][b])
        if(current_race %in% names(varbmalebelo[[current_man]][[current_year]][[current_date]])==FALSE){
          varbmalebelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        belo = append(belo, r11)
        allbelo=append(belo, r11)
      }
    }
  }
  print(current_year)
  for(d in 1:length(varbmalebelo)){
    if(length(varbmalebelo[[d]])>1){
      plastp = varbmalebelo[[d]][[length(varbmalebelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #varbmalebelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      varbmalebelo[[d]][[current_year]][[current_date]] <- list()
      varbmalebelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
varbladybelo <- savevarbladybelo
place_index = 1
allbelo <- c()
for(z in 1:length(varbladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(varbladyraces[[z]])){
    
    
    for(b in 1:length(varbladyraces[[z]][[a]])){
      pbelo = c()
      belo = c()
      
      for(c in 1:length(varbladyraces[[z]][[a]][[b]])){
        lastp = varbladybelo[[names(varbladyraces[[z]][[a]][[b]][c])]][[length(varbladybelo[[names(varbladyraces[[z]][[a]][[b]][c])]])]][[length(varbladybelo[[names(varbladyraces[[z]][[a]][[b]][c])]][[length(varbladybelo[[names(varbladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pbelo = append(pbelo, lastpp)                   
      }
      
      race_df <- data.frame(names(varbladyraces[[z]][[a]][[b]]), as.double(varbladiesdf$Place[place_index:(place_index+length(names(varbladyraces[[z]][[a]][[b]]))-1)]), pbelo)
      
      
      place_index = place_index+ length(names(varbladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pbelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pbelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(varbladyraces[[z]][[a]][[b]][c])
        current_year <- names(varbladyraces[z])
        if(current_year %in% names(varbladybelo[[names(varbladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          varbladybelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(varbladyraces[[z]][a])
        if(current_date %in% names(varbladybelo[[names(varbladyraces[[z]][[a]][[b]][c])]][[names(varbladyraces[z])]])==FALSE){
          varbladybelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(varbladyraces[[z]][[a]][b])
        if(current_race %in% names(varbladybelo[[current_man]][[current_year]][[current_date]])==FALSE){
          varbladybelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        belo = append(belo, r11)
        allbelo=append(belo, r11)
      }
    }
  }
  for(d in 1:length(varbladybelo)){
    if(length(varbladybelo[[d]])>1){
      plastp = varbladybelo[[d]][[length(varbladybelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #varbladybelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      varbladybelo[[d]][[current_year]][[current_date]] <- list()
      varbladybelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}

#Now time to make a data frame

#Name, Season, Date, Distance, Elo
varbmalebelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

varbmnames = c()
varblseason = c()
varbmdate = c()
varbmdistance = c()
varbmbelo = c()
for (a in 1:length(varbmalebelo)){
  tick=0
  for(b in 1:length(varbmalebelo[[a]])){
    for(c in 1:length(varbmalebelo[[a]][[b]])){
      for(d in 1:length(varbmalebelo[[a]][[b]][[c]])){
        tick = tick+1
        #varbmnames2 = append(varbmnames2,as.character(names(varbmalebelo[a])))
      }
    }
  }
  varbmnames = append(varbmnames, rep(as.character(names(varbmalebelo[a])), tick))
}


for (a in 1:length(varbmalebelo)){
  for(b in 1:length(varbmalebelo[[a]])){
    for(c in 1:length(varbmalebelo[[a]][[b]])){
      tick=0
      for(d in 1:length(varbmalebelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      varbmdate=append(varbmdate, rep(as.character(names(varbmalebelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(varbmalebelo)){
  for(b in 1:length(varbmalebelo[[a]])){
    for(c in 1:length(varbmalebelo[[a]][[b]])){
      for(d in 1:length(varbmalebelo[[a]][[b]][[c]])){
        varbmdistance=append(varbmdistance, as.character(names(varbmalebelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(varbmalebelo)){
  for(b in 1:length(varbmalebelo[[a]])){
    for(c in 1:length(varbmalebelo[[a]][[b]])){
      for(d in 1:length(varbmalebelo[[a]][[b]][[c]])){
        varbmbelo=append(varbmbelo, as.double(varbmalebelo[[a]][[b]][[c]][[d]]))}}}}


varbmalebelodf <- data.frame(varbmnames, varbmdate, varbmdistance, varbmbelo)

savevarbmnames <- varbmnames
savevarbmdate <- varbmdate
savevarbmdistance <- varbmdistance
savevarbmbelo <- varbmbelo


varbmalebelodf <- data.frame(varbmnames, varbmdate, varbmdistance, varbmbelo)



varbladybelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

varblnames = c()
varblseason = c()
varbldate = c()
varbldistance = c()
varblbelo = c()
for (a in 1:length(varbladybelo)){
  tick=0
  for(b in 1:length(varbladybelo[[a]])){
    for(c in 1:length(varbladybelo[[a]][[b]])){
      for(d in 1:length(varbladybelo[[a]][[b]][[c]])){
        tick = tick+1
        #varblnames2 = append(varblnames2,as.character(names(varbladybelo[a])))
      }
    }
  }
  varblnames = append(varblnames, rep(as.character(names(varbladybelo[a])), tick))
}


for (a in 1:length(varbladybelo)){
  for(b in 1:length(varbladybelo[[a]])){
    for(c in 1:length(varbladybelo[[a]][[b]])){
      tick=0
      for(d in 1:length(varbladybelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      varbldate=append(varbldate, rep(as.character(names(varbladybelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(varbladybelo)){
  for(b in 1:length(varbladybelo[[a]])){
    for(c in 1:length(varbladybelo[[a]][[b]])){
      for(d in 1:length(varbladybelo[[a]][[b]][[c]])){
        varbldistance=append(varbldistance, as.character(names(varbladybelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(varbladybelo)){
  for(b in 1:length(varbladybelo[[a]])){
    for(c in 1:length(varbladybelo[[a]][[b]])){
      for(d in 1:length(varbladybelo[[a]][[b]][[c]])){
        varblbelo=append(varblbelo, as.double(varbladybelo[[a]][[b]][[c]][[d]]))}}}}


varbladybelodf <- data.frame(varblnames, varbldate, varbldistance, varblbelo)




varbmalehighest <-varbmalebelodf[order(-varbmalebelodf$varbmbelo), ]
varbmalehighest_ind <- varbmalehighest[match(unique(varbmalehighest$varbmnames), varbmalehighest$varbmnames), ]
row.names(varbmalehighest_ind) <- 1:length(varbmalehighest_ind[,1])
varbmalehighest_ind[1:25,]

varbladyhighest <- varbladybelodf[order(-varbladybelodf$varblbelo), ]
varbladyhighest_ind <- varbladyhighest[match(unique(varbladyhighest$varblnames), varbladyhighest$varblnames), ]
row.names(varbladyhighest_ind) <- 1:length(varbladyhighest_ind[,1])
varbladyhighest_ind[1:25,]


jtb = varbmalebelodf[which(varbmalebelodf$varbmnames=="Johannes Thingnes Bø"),]
jtb$race = 1:length(jtb[,1])
mf = varbmalebelodf[(varbmalebelodf$varbmnames=="Martin Fourcade"),]
mf$race = 1:length(mf[,1])
oeb = varbmalebelodf[(varbmalebelodf$varbmnames=="Ole Einar Bjørndalen"),]
oeb$race=1:length(oeb[,1])
ehs = varbmalebelodf[(varbmalebelodf$varbmnames=="Emil Hegle Svendsen"),]
ehs$race=1:length(ehs[,1])
jmo <- rbind(jtb, mf, oeb, ehs)
ggplot(jmo, aes(race, as.double(varbmbelo), colour=varbmnames)) +geom_point()


jb = varbmalebelodf[which(varbmalebelodf$varbmnames=="Jake Brown"),]
jb$race = 1:length(jb[,1])
ps = varbmalebelodf[(varbmalebelodf$varbmnames=="Paul Schommer"),]
ps$race = 1:length(ps[,1])
ln = varbmalebelodf[(varbmalebelodf$varbmnames=="Leif Nordgren"),]
ln$race=1:length(ln[,1])
sd = varbmalebelodf[(varbmalebelodf$varbmnames=="Sean Doherty"),]
sd$race=1:length(sd[,1])
usa <- rbind(jb, ps, ln, sd)
ggplot(usa, aes(race, as.double(varbmbelo), colour=varbmnames)) +geom_point()

varbmalebelodf[which(varbmalebelodf$varbmnames=="Jake Brown"),]
varbmalebelodf[which(varbmalebelodf$varbmnames=="Patrick Weatherton"),]


mor = varbladybelodf[which(varbladybelodf$varblnames=="Marte Olsbu Røiseland"),]
mor$race = 1:length(mor[,1])
mor2020 <- mor[which(as.numeric(as.character(mor$varbldate))>20190500), ]
mor2020$race = 1:length(mor2020[,1])
doro = varbladybelodf[which(varbladybelodf$varblnames=="Dorothea Wierer"),]
doro$race = 1:length(doro[,1])
doro2020 <- doro[which(as.numeric(as.character(doro$varbldate))>20190500), ]
doro2020$race = 1:length(doro2020[,1])
hanao = varbladybelodf[which(varbladybelodf$varblnames=="Hanna Öberg"),]
hanao$race = 1:length(hanao[,1])
hanao2020 <- hanao[which(as.numeric(as.character(hanao$varbldate))>20190500), ]
hanao2020$race = 1:length(hanao2020[,1])
herm = varbladybelodf[which(varbladybelodf$varblnames=="Denise Herrmann"),]
herm$race = 1:length(herm[,1])
herm2020 <- herm[which(as.numeric(as.character(herm$varbldate))>20190500), ]
herm2020$race = 1:length(herm2020[,1])
preus = varbladybelodf[which(varbladybelodf$varblnames=="Franziska Preuss"),]
preus$race = 1:length(preus[,1]) 
preus2020 <- preus[which(as.numeric(as.character(preus$varbldate))>20190500), ]
preus2020$race = 1:length(preus2020[,1])
eck = varbladybelodf[which(varbladybelodf$varblnames=="Tiril Eckhoff"),]
eck$race = 1:length(eck[,1]) 
eck2020 <- eck[which(as.numeric(as.character(eck$varbldate))>20190500), ]
eck2020$race = 1:length(eck2020[,1])
btop_wom <- rbind(mor, doro, hanao, herm, preus, eck)
btop_wom20 <- rbind(mor2020, doro2020, hanao2020, herm2020, preus2020, eck2020)
ggplot(btop_wom20, aes(varbldate, as.double(varblbelo), colour=varblnames)) +geom_point()






varbmalebelodf[varbmalebelodf$varbmdate=="20200306",]



varbmlast_race <- varbmalebelodf[varbmalebelodf$varbmdate=="20150500",]
varbmlast_race <- varbmlast_race[order(-varbmlast_race$varbmbelo),]
row.names(varbmlast_race) <- 1:length(varbmlast_race[,1])
varbmlast_race
varbmlast_race[1:25,]
varbmlast_race[varbmlast_race$varbmnames=="Jake Brown", ]

varbllast_race <- varbladybelodf[varbladybelodf$varbldate=="20200500",]
varbllast_race <- varbllast_race[order(-varbllast_race$varblbelo),]
row.names(varbllast_race) <- 1:length(varbllast_race[,1])
varbllast_race[1:25, ]



actvarbmalebelodfnames <- unique(varbmalebelodf$varbmnames)
for(a in 1:length(actvarbmalebelodfnames)){
  actvarbmskier <- varbmalebelodf[varbmalebelodf$varbmnames==actvarbmalebelodfnames[a], ]
  #print(actvarbmskier)
  row.names(actvarbmskier) <- 1:length(actvarbmskier[,1])
  
  #Starting from the back, the last varbmdistance that is not zero.  Then that one plus one is 
  actvarbmraces <- as.numeric(as.character(row.names(actvarbmskier[which(actvarbmskier$varbmdistance!="0"), ])))
  actvarbmlast <- actvarbmskier[1:actvarbmraces[length(actvarbmraces)]+1, ]
  
  if(a==1){
    row.names(actvarbmlast) <- 1:length(actvarbmlast[,1])
    actvarbmalebelodf<-actvarbmlast
  }
  else{
    #print(length(actvarbmalebelodfnames)-a)
    row.names(actvarbmlast) <- (length(actvarbmalebelodf[,1])+1):(length(actvarbmalebelodf[,1])+length(actvarbmlast[,1]))
    actvarbmalebelodf <- rbind(actvarbmalebelodf, actvarbmlast)
  }
}


actvarbladybelodfnames <- unique(varbladybelodf$varblnames)
for(a in 1:length(actvarbladybelodfnames)){
  actvarblskier <- varbladybelodf[varbladybelodf$varblnames==actvarbladybelodfnames[a], ]
  #print(actvarblskier)
  row.names(actvarblskier) <- 1:length(actvarblskier[,1])
  
  #Starting from the back, the last varbldistance that is not zero.  Then that one plus one is 
  actvarblraces <- as.numeric(as.character(row.names(actvarblskier[which(actvarblskier$varbldistance!="0"), ])))
  actvarbllast <- actvarblskier[1:actvarblraces[length(actvarblraces)]+1, ]
  
  if(a==1){
    row.names(actvarbllast) <- 1:length(actvarbllast[,1])
    actvarbladybelodf<-actvarbllast
  }
  else{
    #print(length(actvarbladybelodfnames)-a)
    row.names(actvarbllast) <- (length(actvarbladybelodf[,1])+1):(length(actvarbladybelodf[,1])+length(actvarbllast[,1]))
    actvarbladybelodf <- rbind(actvarbladybelodf, actvarbllast)
  }
}


actvarbmalenation <- c()
actvarbmaleseason <- c()
for(a in unique(sort(actvarbmalebelodf$varbmdate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actvarbmaleseason <- append(actvarbmaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(actvarbmalebelodf[actvarbmalebelodf$varbmdate==a, 1])))
  }
  else{
    actvarbmaleseason <- append(actvarbmaleseason, rep(as.numeric(substr(a, 1, 4)), length(actvarbmalebelodf[actvarbmalebelodf$varbmdate==a, 1])))#myear <- order(-myear$mmbelo)
  }
}

for(a in unique(actvarbmalebelodf$varbmnames)){
  #print(a)
  actvarbmalenation <- append(actvarbmalenation, rep(varbmendf[varbmendf$Name==a,]$Nationality[length(varbmendf[varbmendf$Name==a,]$Nationality)], 
                                               length(actvarbmalebelodf[actvarbmalebelodf$varbmnames==a,1])))
}

actvarbmalebelodf$varbmseason <- NA
actvarbmalebelodf[order(as.character(actvarbmalebelodf$varbmdate)),]$varbmseason <- actvarbmaleseason

actvarbmalebelodf$varbmnation <- actvarbmalenation

actvarbladynation <- c()
actvarbladyseason <- c()
for(a in unique(sort(actvarbladybelodf$varbldate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actvarbladyseason <- append(actvarbladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(actvarbladybelodf[actvarbladybelodf$varbldate==a, 1])))
  }
  else{
    actvarbladyseason <- append(actvarbladyseason, rep(as.numeric(substr(a, 1, 4)), length(actvarbladybelodf[actvarbladybelodf$varbldate==a, 1])))#myear <- order(-myear$mmbelo)
  }
}

for(a in unique(actvarbladybelodf$varblnames)){
  #print(a)
  actvarbladynation <- append(actvarbladynation, rep(varbladiesdf[varbladiesdf$Name==a,]$Nationality[length(varbladiesdf[varbladiesdf$Name==a,]$Nationality)], 
                                               length(actvarbladybelodf[actvarbladybelodf$varblnames==a,1])))
}
actvarbladybelodf$varblnation <- actvarbladynation
#actvarbladybelodf <- as.data.frame(actvarbladybelodf)
actvarbladybelodf$varblseason <- NA
actvarbladybelodf[order(actvarbladybelodf$varbldate),]$varblseason <- (actvarbladyseason)

actvarbmalebelodf[actvarbmalebelodf$varbmnames=="Jake Brown", ]


actvarbmlast_race <- actvarbmalebelodf[actvarbmalebelodf$varbmdate=="20200500",]
actvarbmlast_race <- actvarbmlast_race[order(-actvarbmlast_race$varbmbelo),]
row.names(actvarbmlast_race) <- 1:length(actvarbmlast_race[,1])
#actvarbmlast_race
actvarbmlast_race[1:25,]
actvarbmlast_race[actvarbmlast_race$varbmnames=="Jake Brown", ]

actvarbllast_race <- actvarbladybelodf[actvarbladybelodf$varbldate=="20200500",]
actvarbllast_race <- actvarbllast_race[order(-actvarbllast_race$varblbelo),]
row.names(actvarbllast_race) <- 1:length(actvarbllast_race[,1])
actvarbllast_race[1:25, ]


varbmseasonstandings <- actvarbmalebelodf[which(endsWith(as.character(actvarbmalebelodf$varbmdate), "0500") ), ]
varbmseasonstandings <- varbmseasonstandings[order((as.character(varbmseasonstandings$varbmdate))), ]
varbmrank <- c()
for(a in unique(varbmseasonstandings$varbmdate)){
  varbmseasonstandings[varbmseasonstandings$varbmdate==a, ] <- varbmseasonstandings[varbmseasonstandings$varbmdate==a, ][order(-varbmseasonstandings[varbmseasonstandings$varbmdate==a, ]$varbmbelo), ]
  #varbmyear <- order(-varbmyear$varbmmelo)
  varbmrank <- append(varbmrank, 1:length(varbmseasonstandings[varbmseasonstandings$varbmdate==a, 1]))
}
varbmseasonstandings$varbmrank <- varbmrank
row.names(varbmseasonstandings)<-1:length(varbmseasonstandings[,1])
varbmseasonstandings[varbmseasonstandings$varbmdate=="20200500", ]
varbmseasonstandings[varbmseasonstandings$varbmnames=="Martin Fourcade", ]
varbmusa <- (varbmseasonstandings[varbmseasonstandings$varbmnation=="USA", ])


varblseasonstandings <- actvarbladybelodf[which(endsWith(as.character(actvarbladybelodf$varbldate), "0500") ), ]
varblseasonstandings <- varblseasonstandings[order((as.character(varblseasonstandings$varbldate))), ]
varblrank <- c()
for(a in unique(varblseasonstandings$varbldate)){
  varblseasonstandings[varblseasonstandings$varbldate==a, ] <- varblseasonstandings[varblseasonstandings$varbldate==a, ][order(-varblseasonstandings[varblseasonstandings$varbldate==a, ]$varblbelo), ]
  #varblyear <- order(-varblyear$varblmelo)
  varblrank <- append(varblrank, 1:length(varblseasonstandings[varblseasonstandings$varbldate==a, 1]))
}
varblseasonstandings$varblrank <- varblrank
row.names(varblseasonstandings)<-1:length(varblseasonstandings[,1])
varblseasonstandings[varblseasonstandings$varbldate=="20200500", ]
varblseasonstandings[varblseasonstandings$varblnames=="Magdalena Forsberg", ]
varblusa <- (varblseasonstandings[varblseasonstandings$varblnation=="USA", ])


ggplot(data=varbmusa, aes(x=varbmseason, y=0-varbmrank, group=varbmnames, color=varbmnames)) + geom_line() + geom_point()
ggplot(data=varblusa, aes(x=varblseason, y=0-varblrank, group=varblnames, color=varblnames)) + geom_line() + geom_point()





varbmlast_race <- varbmalebelodf[varbmalebelodf$varbmdate=="20201205",]
#varmalelast_race <- varmalelast_race[varmalelast_race$varmaledistance=="Stage", ]
varbmlast_race <- varbmlast_race[order(-varbmlast_race$varbmbelo),]
row.names(varbmlast_race) <- 1:length(varbmlast_race[,1])
varbmlast_race[1:25,]

varbllast_race <- varbladybelodf[varbladybelodf$varbldate=="20201206",]
#varladylast_race <- varladylast_race[varladylast_race$varladydistance=="Stage", ]
varbllast_race <- varbllast_race[order(-varbllast_race$varblbelo),]
row.names(varbllast_race) <- 1:length(varbllast_race[,1])
varbllast_race[1:25,]



save_varbmalebelodf <- varbmalebelodf
save_varbladybelodf <- varbladybelodf
