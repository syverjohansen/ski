library(readxl)
sprintmen <- read_excel("~/ski/elo/excel365/all.xlsx", 
                  sheet = "Men", col_names = FALSE, na = "NA")
sprintladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                     sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
sprintmendf <- data.frame(sprintmen)
names(sprintmendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
sprintmendf$Seasons = NA

malesprintdistances <-c("Sprint") 
sprintmendf <- sprintmendf[as.character(sprintmendf$Distance) %in% malesprintdistances, ]
row.names(sprintmendf) <- 1:length(sprintmendf[,1])
sprintladiesdf <- data.frame(sprintladies)
names(sprintladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
sprintladiesdf$Seasons = NA

ladysprintdistances <- c("Sprint")


sprintladiesdf <- sprintladiesdf[as.character(sprintladiesdf$Distance) %in% ladysprintdistances, ]
row.names(sprintladiesdf) <- 1:length(sprintladiesdf[,1])
sprintladiesdf[4721, 9] <- "Tatjana Kuznetsova2"

for(a in 1:length(sprintmendf$Date)){
  if((as.double(substr(sprintmendf$Date[a], 5, 8)) > 1000) && as.double(substr(sprintmendf$Date[a],5,8)!=9999)){
    sprintmendf$Seasons[a] = as.character(as.double(substr(sprintmendf$Date[a], 1, 4))+1)
  }
  else{
    sprintmendf$Seasons[a] = as.character(as.double(substr(sprintmendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(sprintladiesdf$Date)){
  if((as.double(substr(sprintladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(sprintladiesdf$Date[a],5,8)!=9999)){
    sprintladiesdf$Seasons[a] = as.character(as.double(substr(sprintladiesdf$Date[a], 1, 4))+1)
  }
  else{
    sprintladiesdf$Seasons[a] = as.character(as.double(substr(sprintladiesdf$Date[a], 1, 4)))
  }
}

sprintmaleraces = list()
sprintladyraces = list()

for(a in 1:length(sprintmendf$Seasons)){
  if(sprintmendf$Seasons[a] %in% names(sprintmaleraces)==FALSE){
    sprintmaleraces[[sprintmendf$Seasons[a]]] <- list()
  }
  
  if(sprintmendf$Date[a] %in% names(sprintmaleraces[[sprintmendf$Seasons[a]]])==FALSE){
    sprintmaleraces[[sprintmendf$Seasons[a]]][[sprintmendf$Date[a]]] <- list()
  }
  if(sprintmendf$Distance[a] %in% names(sprintmaleraces[[sprintmendf$Seasons[a]]][[sprintmendf$Date[a]]])==FALSE){
    sprintmaleraces[[sprintmendf$Seasons[a]]][[sprintmendf$Date[a]]][[sprintmendf$Distance[a]]]<-list()
  }
  if(sprintmendf$Name[a] %in% names(sprintmaleraces[[sprintmendf$Seasons[a]]][[sprintmendf$Date[a]]][[sprintmendf$Distance[a]]])==FALSE){
    sprintmaleraces[[sprintmendf$Seasons[a]]][[sprintmendf$Date[a]]][[sprintmendf$Distance[a]]][[sprintmendf$Name[a]]]<-as.double(sprintmendf$Place[a])
  }
}

for(a in 1:length(sprintladiesdf$Seasons)){
  if(sprintladiesdf$Seasons[a] %in% names(sprintladyraces)==FALSE){
    sprintladyraces[[sprintladiesdf$Seasons[a]]] <- list()
  }
  
  if(sprintladiesdf$Date[a] %in% names(sprintladyraces[[sprintladiesdf$Seasons[a]]])==FALSE){
    sprintladyraces[[sprintladiesdf$Seasons[a]]][[sprintladiesdf$Date[a]]] <- list()
  }
  if(sprintladiesdf$Distance[a] %in% names(sprintladyraces[[sprintladiesdf$Seasons[a]]][[sprintladiesdf$Date[a]]])==FALSE){
    sprintladyraces[[sprintladiesdf$Seasons[a]]][[sprintladiesdf$Date[a]]][[sprintladiesdf$Distance[a]]]<-list()
  }
  if(sprintladiesdf$Name[a] %in% names(sprintladyraces[[sprintladiesdf$Seasons[a]]][[sprintladiesdf$Date[a]]][[sprintladiesdf$Distance[a]]])==FALSE){
    sprintladyraces[[sprintladiesdf$Seasons[a]]][[sprintladiesdf$Date[a]]][[sprintladiesdf$Distance[a]]][[sprintladiesdf$Name[a]]]<-as.double(sprintladiesdf$Place[a])
  }
}

savesprintmaleraces<-sprintmaleraces
savesprintladyraces <- sprintladyraces

#Initialize all skiers to have an Elo of 1300
sprintmaleelo = list()
sprintladyelo = list()
for(a in 1:length(sprintmendf$Name)){
  if(sprintmendf$Name[a] %in% names(sprintmaleelo) == FALSE){
    sprintmaleelo[[sprintmendf$Name[a]]] <- list()
    sprintmaleelo[[sprintmendf$Name[a]]][["0000"]]<-list()
    sprintmaleelo[[sprintmendf$Name[a]]][["0000"]][["00000000"]]<-list()
    sprintmaleelo[[sprintmendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(sprintladiesdf$Name)){
  if(sprintladiesdf$Name[a] %in% names(sprintladyelo) == FALSE){
    sprintladyelo[[sprintladiesdf$Name[a]]] <- list()
    sprintladyelo[[sprintladiesdf$Name[a]]][["0000"]]<-list()
    sprintladyelo[[sprintladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    sprintladyelo[[sprintladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savesprintmaleelo<-sprintmaleelo
savesprintladyelo <- sprintladyelo

sprintmaleelo<-savesprintmaleelo

allelo <- c()
K=1
place_index = 1

for(z in 1:length(sprintmaleraces)){
  print(z)
  
  for(a in 1:length(sprintmaleraces[[z]])){
    
    
    for(b in 1:length(sprintmaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(sprintmaleraces[[z]][[a]][[b]])){
        lastp = sprintmaleelo[[names(sprintmaleraces[[z]][[a]][[b]][c])]][[length(sprintmaleelo[[names(sprintmaleraces[[z]][[a]][[b]][c])]])]][[length(sprintmaleelo[[names(sprintmaleraces[[z]][[a]][[b]][c])]][[length(sprintmaleelo[[names(sprintmaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(sprintmaleraces[[z]][[a]][[b]]), as.double(sprintmendf$Place[place_index:(place_index+length(names(sprintmaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(sprintmaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(sprintmaleraces[[z]][[a]][[b]][c])
        current_year <- names(sprintmaleraces[z])
        if(current_year %in% names(sprintmaleelo[[names(sprintmaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          sprintmaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(sprintmaleraces[[z]][a])
        if(current_date %in% names(sprintmaleelo[[names(sprintmaleraces[[z]][[a]][[b]][c])]][[names(sprintmaleraces[z])]])==FALSE){
          sprintmaleelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(sprintmaleraces[[z]][[a]][b])
        if(current_race %in% names(sprintmaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          sprintmaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(sprintmaleelo)){
    if(length(sprintmaleelo[[d]])>1){
      plastp = sprintmaleelo[[d]][[length(sprintmaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #sprintmaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      sprintmaleelo[[d]][[current_year]][[current_date]] <- list()
      sprintmaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
sprintladyelo <- savesprintladyelo
place_index = 1
allelo <- c()
for(z in 1:length(sprintladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(sprintladyraces[[z]])){
    
    
    for(b in 1:length(sprintladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(sprintladyraces[[z]][[a]][[b]])){
        lastp = sprintladyelo[[names(sprintladyraces[[z]][[a]][[b]][c])]][[length(sprintladyelo[[names(sprintladyraces[[z]][[a]][[b]][c])]])]][[length(sprintladyelo[[names(sprintladyraces[[z]][[a]][[b]][c])]][[length(sprintladyelo[[names(sprintladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(sprintladyraces[[z]][[a]][[b]]), as.double(sprintladiesdf$Place[place_index:(place_index+length(names(sprintladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(sprintladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(sprintladyraces[[z]][[a]][[b]][c])
        current_year <- names(sprintladyraces[z])
        if(current_year %in% names(sprintladyelo[[names(sprintladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          sprintladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(sprintladyraces[[z]][a])
        if(current_date %in% names(sprintladyelo[[names(sprintladyraces[[z]][[a]][[b]][c])]][[names(sprintladyraces[z])]])==FALSE){
          sprintladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(sprintladyraces[[z]][[a]][b])
        if(current_race %in% names(sprintladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          sprintladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        allelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(sprintladyelo)){
    if(length(sprintladyelo[[d]])>1){
      plastp = sprintladyelo[[d]][[length(sprintladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #sprintladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      sprintladyelo[[d]][[current_year]][[current_date]] <- list()
      sprintladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}


sprintmaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

sprintmalenames = c()
blseason = c()
sprintmaledate = c()
sprintmaledistance = c()
sprintmaleeloscore = c()
for (a in 1:length(sprintmaleelo)){
  tick=0
  for(b in 1:length(sprintmaleelo[[a]])){
    for(c in 1:length(sprintmaleelo[[a]][[b]])){
      for(d in 1:length(sprintmaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #sprintmalenames2 = append(sprintmalenames2,as.character(names(sprintmaleelo[a])))
      }
    }
  }
  sprintmalenames = append(sprintmalenames, rep(as.character(names(sprintmaleelo[a])), tick))
}


for (a in 1:length(sprintmaleelo)){
  for(b in 1:length(sprintmaleelo[[a]])){
    for(c in 1:length(sprintmaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(sprintmaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      sprintmaledate=append(sprintmaledate, rep(as.character(names(sprintmaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(sprintmaleelo)){
  for(b in 1:length(sprintmaleelo[[a]])){
    for(c in 1:length(sprintmaleelo[[a]][[b]])){
      for(d in 1:length(sprintmaleelo[[a]][[b]][[c]])){
        sprintmaledistance=append(sprintmaledistance, as.character(names(sprintmaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(sprintmaleelo)){
  for(b in 1:length(sprintmaleelo[[a]])){
    for(c in 1:length(sprintmaleelo[[a]][[b]])){
      for(d in 1:length(sprintmaleelo[[a]][[b]][[c]])){
        sprintmaleeloscore=append(sprintmaleeloscore, as.double(sprintmaleelo[[a]][[b]][[c]][[d]]))}}}}


sprintmaleelodf <- data.frame(sprintmalenames, sprintmaledate, sprintmaledistance, sprintmaleeloscore)



sprintladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

sprintladynames = c()
blseason = c()
sprintladydate = c()
sprintladydistance = c()
sprintladyeloscore = c()
for (a in 1:length(sprintladyelo)){
  tick=0
  for(b in 1:length(sprintladyelo[[a]])){
    for(c in 1:length(sprintladyelo[[a]][[b]])){
      for(d in 1:length(sprintladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #sprintladynames2 = append(sprintladynames2,as.character(names(sprintladyelo[a])))
      }
    }
  }
  sprintladynames = append(sprintladynames, rep(as.character(names(sprintladyelo[a])), tick))
}


for (a in 1:length(sprintladyelo)){
  for(b in 1:length(sprintladyelo[[a]])){
    for(c in 1:length(sprintladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(sprintladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      sprintladydate=append(sprintladydate, rep(as.character(names(sprintladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(sprintladyelo)){
  for(b in 1:length(sprintladyelo[[a]])){
    for(c in 1:length(sprintladyelo[[a]][[b]])){
      for(d in 1:length(sprintladyelo[[a]][[b]][[c]])){
        sprintladydistance=append(sprintladydistance, as.character(names(sprintladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(sprintladyelo)){
  for(b in 1:length(sprintladyelo[[a]])){
    for(c in 1:length(sprintladyelo[[a]][[b]])){
      for(d in 1:length(sprintladyelo[[a]][[b]][[c]])){
        sprintladyeloscore=append(sprintladyeloscore, as.double(sprintladyelo[[a]][[b]][[c]][[d]]))}}}}


sprintladyelodf <- data.frame(sprintladynames, sprintladydate, sprintladydistance, sprintladyeloscore)

savesprintmalenames <- sprintmalenames
savesprintmaledistance <- sprintmaledistance
savesprintmaledate <- sprintmaledate
savesprintmaleeloscore <- sprintmaleeloscore
savesprintladynames <- sprintladynames
savesprintladydistance <- sprintladydistance
savesprintladydate <- sprintladydate
savesprintladyeloscore <- sprintladyeloscore


sprintmalehighest <- sprintmaleelodf[order(-sprintmaleelodf$sprintmaleeloscore), ]
sprintmalehighest_ind <- sprintmalehighest[match(unique(sprintmalehighest$sprintmalenames), sprintmalehighest$sprintmalenames), ]
row.names(sprintmalehighest_ind) <- 1:length(sprintmalehighest_ind[,1])
sprintmalehighest_ind[1:25,]


sprintladyhighest <- sprintladyelodf[order(-sprintladyelodf$sprintladyeloscore), ]
sprintladyhighest_ind <-sprintladyhighest[match(unique(sprintladyhighest$sprintladynames), sprintladyhighest$sprintladynames), ]
row.names(sprintladyhighest_ind) <- 1:length(sprintladyhighest_ind[,1])
sprintladyhighest_ind[1:25, ]

sprintmalelast_race <- sprintmaleelodf[sprintmaleelodf$sprintmaledate=="20200500",]
sprintmalelast_race[order(-sprintmalelast_race$sprintmaleeloscore),]

library(ggplot2)

jhk = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(sprintmaleeloscore), colour=sprintmalenames)) +geom_point()


bd = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = sprintmaleelodf[(sprintmaleelodf$sprintmalenames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(sprintmaleeloscore), colour=sprintmalenames)) +geom_point()


sprintmaleelodf[which(sprintmaleelodf$sprintmalenames=="Kevin Brochman"),]

jd = sprintladyelodf[(sprintladyelodf$sprintladynames)=="Jessica Diggins", ]
kr = sprintladyelodf[(sprintladyelodf$sprintladynames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(sprintladyeloscore), colour=sprintladynames)) +geom_point()

tj = sprintladyelodf[(sprintladyelodf$sprintladynames)=="Therese Johaug", ]
mb = sprintladyelodf[(sprintladyelodf$sprintladynames)=="Marit Bjørgen", ]
io = sprintladyelodf[(sprintladyelodf$sprintladynames)=="Ingvild Flugstad Østberg", ]
hw = sprintladyelodf[(sprintladyelodf$sprintladynames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(sprintladyeloscore), colour=sprintladynames)) +geom_point()


sprintladyelodf[(sprintladyelodf$sprintladynames)=="Rosie Brennan", ]


sprintmalelast_race <- sprintmaleelodf[sprintmaleelodf$sprintmaledate=="20200500",]
sprintmalelast_race <- sprintmalelast_race[order(-sprintmalelast_race$sprintmaleeloscore),]
row.names(sprintmalelast_race) <- 1:length(sprintmalelast_race[,1])
sprintmalelast_race[1:25,]

sprintladylast_race <- sprintladyelodf[sprintladyelodf$sprintladydate=="20200500", ]
sprintladylast_race <- sprintladylast_race[order(-sprintladylast_race$sprintladyeloscore),]
row.names(sprintladylast_race) <- 1:length(sprintladylast_race[,1])
sprintladylast_race[1:25,]





actsprintmaleelodfnames <- unique(sprintmaleelodf$sprintmalenames)
for(a in 1:length(actsprintmaleelodfnames)){
  actsprintmaleskier <- sprintmaleelodf[sprintmaleelodf$sprintmalenames==actsprintmaleelodfnames[a], ]
  #print(actsprintmaleskier)
  row.names(actsprintmaleskier) <- 1:length(actsprintmaleskier[,1])
  
  #Starting from the back, the last sprintmaledistance that is not zero.  Then that one plus one is 
  actsprintmaleraces <- as.numeric(as.character(row.names(actsprintmaleskier[which(actsprintmaleskier$sprintmaledistance!="0"), ])))
  actsprintmalelast <- actsprintmaleskier[1:actsprintmaleraces[length(actsprintmaleraces)]+1, ]
  
  if(a==1){
    row.names(actsprintmalelast) <- 1:length(actsprintmalelast[,1])
    actsprintmaleelodf<-actsprintmalelast
  }
  else{
    #print(length(actsprintmaleelodfnames)-a)
    row.names(actsprintmalelast) <- (length(actsprintmaleelodf[,1])+1):(length(actsprintmaleelodf[,1])+length(actsprintmalelast[,1]))
    actsprintmaleelodf <- rbind(actsprintmaleelodf, actsprintmalelast)
  }
}


actsprintladyelodfnames <- unique(sprintladyelodf$sprintladynames)
for(a in 1:length(actsprintladyelodfnames)){
  actsprintladyskier <- sprintladyelodf[sprintladyelodf$sprintladynames==actsprintladyelodfnames[a], ]
  #print(actsprintladyskier)
  row.names(actsprintladyskier) <- 1:length(actsprintladyskier[,1])
  
  #Starting from the back, the last sprintladydistance that is not zero.  Then that one plus one is 
  actsprintladyraces <- as.numeric(as.character(row.names(actsprintladyskier[which(actsprintladyskier$sprintladydistance!="0"), ])))
  actsprintladylast <- actsprintladyskier[1:actsprintladyraces[length(actsprintladyraces)]+1, ]
  
  if(a==1){
    row.names(actsprintladylast) <- 1:length(actsprintladylast[,1])
    actsprintladyelodf<-actsprintladylast
  }
  else{
    #print(length(actsprintladyelodfnames)-a)
    row.names(actsprintladylast) <- (length(actsprintladyelodf[,1])+1):(length(actsprintladyelodf[,1])+length(actsprintladylast[,1]))
    actsprintladyelodf <- rbind(actsprintladyelodf, actsprintladylast)
  }
}
actsprintmalenation <- c()
actsprintmaleseason <- c()
for(a in unique(sort(actsprintmaleelodf$sprintmaledate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actsprintmaleseason <- append(actsprintmaleseason, rep(as.numeric(substr(a, 1, 4))+1, length(actsprintmaleelodf[actsprintmaleelodf$sprintmaledate==a, 1])))
  }
  else{
    actsprintmaleseason <- append(actsprintmaleseason, rep(as.numeric(substr(a, 1, 4)), length(actsprintmaleelodf[actsprintmaleelodf$sprintmaledate==a, 1])))#myear <- order(-myear$msprintmaleeloscore)
  }
}

for(a in unique(actsprintmaleelodf$sprintmalenames)){
  #print(a)
  actsprintmalenation <- append(actsprintmalenation, rep(mendf[mendf$Name==a,]$Nationality[length(mendf[mendf$Name==a,]$Nationality)], 
                                                         length(actsprintmaleelodf[actsprintmaleelodf$sprintmalenames==a,1])))
}
actsprintmaleelodf$sprintmaleseason <- NA
actsprintmaleelodf[order(as.character(actsprintmaleelodf$sprintmaledate)),]$sprintmaleseason <- actsprintmaleseason
actsprintmaleelodf$sprintmalenation <- actsprintmalenation

actsprintladynation <- c()
actsprintladyseason <- c()
for(a in unique(sort(actsprintladyelodf$sprintladydate))){
  if(as.numeric(as.character(substr(a,5,8)))>500){
    #print(a)
    actsprintladyseason <- append(actsprintladyseason, rep(as.numeric(substr(a, 1, 4))+1, length(actsprintladyelodf[actsprintladyelodf$sprintladydate==a, 1])))
  }
  else{
    actsprintladyseason <- append(actsprintladyseason, rep(as.numeric(substr(a, 1, 4)), length(actsprintladyelodf[actsprintladyelodf$sprintladydate==a, 1])))#myear <- order(-myear$msprintmaleeloscore)
  }
}

for(a in unique(actsprintladyelodf$sprintladynames)){
  #print(a)
  actsprintladynation <- append(actsprintladynation, rep(sprintladiesdf[sprintladiesdf$Name==a,]$Nationality[length(sprintladiesdf[sprintladiesdf$Name==a,]$Nationality)], 
                                                         length(actsprintladyelodf[actsprintladyelodf$sprintladynames==a,1])))
}
actsprintladyelodf$sprintladynation <- actsprintladynation
#actsprintladyelodf <- as.data.frame(actsprintladyelodf)
actsprintladyelodf$sprintladyseason <- NA
actsprintladyelodf[order(actsprintladyelodf$sprintladydate),]$sprintladyseason <- (actsprintladyseason)


actsprintmaleelodf[actsprintmaleelodf$sprintmalenames=="Johannes Høsflot Klæbo", ]
actsprintladyelodf[actsprintladyelodf$sprintladynames=="Jessica Diggins", ]

actsprintmalelast_race <- actsprintmaleelodf[actsprintmaleelodf$sprintmaledate=="20200500",]
actsprintmalelast_race <- actsprintmalelast_race[order(-actsprintmalelast_race$sprintmaleeloscore),]
row.names(actsprintmalelast_race) <- 1:length(actsprintmalelast_race[,1])
#actsprintmalelast_race
actsprintmalelast_race[1:25,]
actsprintmalelast_race[actsprintmalelast_race$sprintmalenames=="Dario Cologna", ]

actsprintladylast_race <- actsprintladyelodf[actsprintladyelodf$sprintladydate=="20200500",]
actsprintladylast_race <- actsprintladylast_race[order(-actsprintladylast_race$sprintladyeloscore),]
row.names(actsprintladylast_race) <- 1:length(actsprintladylast_race[,1])
actsprintladylast_race[1:25, ]
actsprintladylast_race[actsprintladylast_race$sprintladynames=="Jessica Diggins", ]


sprintmaleseasonstandings <- actsprintmaleelodf[which(endsWith(as.character(actsprintmaleelodf$sprintmaledate), "0500") ), ]
sprintmaleseasonstandings <- sprintmaleseasonstandings[order((as.character(sprintmaleseasonstandings$sprintmaledate))), ]
sprintmalerank <- c()

for(a in unique(sprintmaleseasonstandings$sprintmaledate)){
  sprintmaleseasonstandings[sprintmaleseasonstandings$sprintmaledate==a, ] <- sprintmaleseasonstandings[sprintmaleseasonstandings$sprintmaledate==a, ][order(-sprintmaleseasonstandings[sprintmaleseasonstandings$sprintmaledate==a, ]$sprintmaleeloscore), ]
  sprintmalerank <- append(sprintmalerank, 1:length(sprintmaleseasonstandings[sprintmaleseasonstandings$sprintmaledate==a, 1]))
}
sprintmaleseasonstandings$sprintmalerank <- sprintmalerank
row.names(sprintmaleseasonstandings)<-1:length(sprintmaleseasonstandings[,1])
mwrong_names <- c()
# for(a in unique(sprintmaleseasonstandings$sprintmalenames)){
#   #sprintmaleseasonstandings[sprintmaleseasonstandings$sprintmalenames==a,] <- mendf
#   temp_nation <- mendf[mendf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     mwrong_names <- append(mwrong_names, a)
#   }
#   #if(length(sprintmaleseasonstandings[sprintmaleseasonstandings$sprintmalenames==a,1])>15){
#   #  print(a)
#   #}
#   sprintmaleseason_diff <- mendf[mendf$Name==a,]
#   #sprintmaleseasonnation <- append(sprintmaleseasonnation, mendf[mendf$Name==a,])
# }
# mwrong_names <- mwrong_names[! mwrong_names %in% c("Mikhail Devjatiarov", "Vladimir Smirnov", "Alexandr Uschkalenko", "Alexei Prokourorov", 
#                                 "Igor Badamchin", "Aleksandr Golubev", "Urmas Välbe", "Andrej Kirillov", "German Karatschewskij",
#                                 "Viatscheslav Plaksunov", "Gennadiy Lasutin", "Igor Obukhov", "Elmo Kassin", "Alexander Karatschewskij")]


sprintmaleseasonstandings[sprintmaleseasonstandings$sprintmaledate=="20200500", ]
# View(sprintmaleseasonstandings[sprintmaleseasonstandings$sprintmalenames=="Gunde Svan", ])
# View(mendf[mendf$Name=="Gunde Svan", ])
# View(sprintmaleseasonstandings[sprintmaleseasonstandings$sprintmalenation=="Norway",])
# View(sprintmaleseasonstandings)

sprintladyseasonstandings <- actsprintladyelodf[which(endsWith(as.character(actsprintladyelodf$sprintladydate), "0500") ), ]
sprintladyseasonstandings <- sprintladyseasonstandings[order((as.character(sprintladyseasonstandings$sprintladydate))), ]
sprintladyrank <- c()
for(a in unique(sprintladyseasonstandings$sprintladydate)){
  sprintladyseasonstandings[sprintladyseasonstandings$sprintladydate==a, ] <- 
    sprintladyseasonstandings[sprintladyseasonstandings$sprintladydate==a, ][order(-sprintladyseasonstandings[sprintladyseasonstandings$sprintladydate==a, ]$sprintladyeloscore), ]
  #lyear <- order(-lyear$lsprintmaleeloscore)
  sprintladyrank <- append(sprintladyrank, 1:length(sprintladyseasonstandings[sprintladyseasonstandings$sprintladydate==a, 1]))
}
sprintladyseasonstandings$sprintladyrank <- sprintladyrank
row.names(sprintladyseasonstandings)<-1:length(sprintladyseasonstandings[,1])


lwrong_names <- c()
# for(a in unique(sprintladyseasonstandings$sprintladynames)){
#   #sprintmaleseasonstandings[sprintmaleseasonstandings$sprintmalenames==a,] <- sprintladiesdf
#   temp_nation <- sprintladiesdf[sprintladiesdf$Name==a,]$Nationality
#   if(length(unique(temp_nation))!=1){
#     lwrong_names <- append(lwrong_names, a)
#   }
#  # if(length(sprintladyseasonstandings[sprintladyseasonstandings$sprintladynames==a,1])>20){
#   #  print(a)
#  # }
#   sprintladyseason_diff <- sprintladiesdf[sprintladiesdf$Name==a,]
#   #sprintmaleseasonnation <- append(sprintmaleseasonnation, sprintladiesdf[sprintladiesdf$Name==a,])
# }


sprintladyseasonstandings[sprintladyseasonstandings$sprintladydate=="20200500", ]
sprintladyseasonstandings[sprintladyseasonstandings$sprintladynames=="Jessica Diggins", ]
View(sprintladiesdf[sprintladiesdf$Name=="Lilia Vasilieva", ])
View(sprintladyseasonstandings[sprintladyseasonstandings$sprintladynation=="USA",])
View(sprintladyseasonstandings)





