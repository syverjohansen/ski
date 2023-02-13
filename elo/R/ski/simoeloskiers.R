library("tidyverse")
library(simstudy)
library(psych)
library(SuppDists)
library(truncdist)
library(truncnorm)
library(EnvStats)
library(e1071)
simmen <- read_excel("~/ski/elo/excel365/all.xlsx", 
                  sheet = "Men", col_names = FALSE, na = "NA")
simladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                     sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
simmendf <- data.frame(simmen)
names(simmendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
simladiesdf <- data.frame(simladies)
names(simladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")

simmendf[2073,9] <- "Oddmund Jensen2"
simmendf[which(simmendf$Distance=="Stage"), 1] <- as.character(as.double(simmendf[which(simmendf$Distance=="Stage"), 1])+1)
simladiesdf[which(simladiesdf$Distance=="Stage"), 1] <- as.character(as.double(simladiesdf[which(simladiesdf$Distance=="Stage"), 1])+1)



  simmendf$Seasons = NA
  simladiesdf$Seasons = NA
  for(a in 1:length(simmendf$Date)){
  if((as.double(substr(simmendf$Date[a], 5, 8)) > 1000) && as.double(substr(simmendf$Date[a],5,8)!=9999)){
    simmendf$Seasons[a] = as.character(as.double(substr(simmendf$Date[a], 1, 4))+1)
  }
  else{
    simmendf$Seasons[a] = as.character(as.double(substr(simmendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(simladiesdf$Date)){
  if((as.double(substr(simladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(simladiesdf$Date[a],5,8)!=9999)){
    simladiesdf$Seasons[a] = as.character(as.double(substr(simladiesdf$Date[a], 1, 4))+1)
  }
  else{
    simladiesdf$Seasons[a] = as.character(as.double(substr(simladiesdf$Date[a], 1, 4)))
  }
}

simladiesdf[24331, 9] <- "Tatjana Kuznetsova2"

for(a in 1982:2020){
  print(a)
  cur_seasondf <- simmendf[simmendf$Seasons==as.character(a), ]
  #
  num_racers <- as.numeric(as.matrix(table(cur_seasondf$Date)))
  num_races <- length(unique(cur_seasondf$Date))
  num_simraces <- 38-num_races
  

 # print(table(name_pot))
  sim_results <- c()
  for(z in 1: num_simraces){
    name_pot <- cur_seasondf$Name
   # print(table(name_pot))
    name_pot_del <- names(table(name_pot)[which(as.numeric(as.matrix(table(name_pot)))<2)])
    #print(name_pot_del)
    name_pot = name_pot[! name_pot %in% name_pot_del]
    #print(length(unique(name_pot)))
    
    if(max(num_racers)+1 > length(unique(name_pot))){
      simnum_racers <- as.integer(rtruncnorm(1, min(num_racers), length(unique(name_pot)), mean(num_racers), sd(num_racers)))
    }
    else{
    simnum_racers <- as.integer(rtruncnorm(1, min(num_racers), max(num_racers)+1, mean(num_racers), sd(num_racers)))
    }
    #print(simnum_racers)
    
   
  
  
 name_sample <- c()
  for(b in 1:simnum_racers){
    sim_racer <- sample(name_pot, 1)
 
    #print(sim_racer)
    name_sample <- append(name_sample, sim_racer)
    name_pot <- name_pot[! name_pot %in% sim_racer]
    #setdiff[name_pot, sim_racer]
  }
 #print(name_sample)
  #print(name_sample)
  
  (cur_seasondf[cur_seasondf$Name=="Sung-Hoon Cho", ])
  #cur_seasondf[which(cur_seasondf$Name %in% name_sample), ]
  name_sample <- (name_sample[order(name_sample)])
 # print(name_sample)
  pnorm(1, mean=mean(as.double(cur_seasondf[name_sample,8])), 
        sd=sd(as.double(cur_seasondf[name_sample,8])))
 
  results = c()
  sim_nation <- c()
  for(c in 1:(simnum_racers)){
    pos_prob_tot=0
    raw_pnorms = c()
    
  for(d in 1:length(name_sample)){
   
    raw_pnorm <- pnorm(c, mean=mean(as.double(cur_seasondf[cur_seasondf$Name==name_sample[d],8])), 
                       sd=sd(as.double(cur_seasondf[cur_seasondf$Name==name_sample[d],8])))
  # print(name_sample[d])
  # print(raw_pnorm)
   # print(raw_pnorm)
    raw_pnorms <- append(raw_pnorms, raw_pnorm)
    pos_prob_tot <- pos_prob_tot +pnorm(c, mean=mean(as.double(cur_seasondf[cur_seasondf$Name==name_sample[d],8])), 
                                        sd=sd(as.double(cur_seasondf[cur_seasondf$Name==name_sample[d],8])))
    #print(name_sample[c])
    #print(pnorm(1, mean=mean(as.double(cur_seasondf[cur_seasondf$Name==name_sample[c],8])), 
                #sd=sd(as.double(cur_seasondf[cur_seasondf$Name==name_sample[c],8]))))
    
  }
  adj_pnorms <- raw_pnorms/pos_prob_tot
  if(is.na(adj_pnorms[length(adj_pnorms)])){
    pos <- name_sample[length(adj_pnorms)]
  }
  else{
  pos <- sample(name_sample, 1, prob=adj_pnorms)
  }
  sim_nation = (append(sim_nation, cur_seasondf[which(cur_seasondf$Name==pos)[1], 10]))
 # print(cur_seasondf[which(cur_seasondf$Name==pos)[1], 10])
  #print(name_sample)
  #print(pos)
  name_sample <- name_sample[! name_sample %in% pos]
  results = append(results, pos)
  #print(results)
  }
  results_names <- results
  results_pos <- 1:length(results_names)
  #print(results_names)
  results_city <- rep("Sim City", length(results_names))
  results_country <- rep("Simland", length(results_names))
  results_event <- rep("Sim", length(results_names))
  results_sex <- rep("M", length(results_names))
  results_distance <- rep("Sim", length(results_names))
  results_discipline <- rep("Sim", length(results_names))
  results_date <- rep(as.character(as.double(cur_seasondf[length(cur_seasondf$Date), 1])+1), length(results_names))
  results_season <- rep(a, length(results_names))
  
  
  sim_results<-data.frame(as.character(results_date), as.character(results_city), as.character(results_country), as.character(results_event), as.character(results_sex), 
                          as.character(results_distance), as.character(results_discipline), as.character(results_pos), as.character(results_names),
                          as.character(sim_nation), as.character(results_season))
  
  names(sim_results) <- names(cur_seasondf)
  #print(sim_results)
  
  cur_seasondf <- rbind(cur_seasondf, sim_results)
  #sim_results = append(sim_results, results)
  }
  #Box cox if you need it later
  # box.cox <- boxcox(as.double(cur_seasondf[cur_seasondf$Name=="Johannes Høsflot Klæbo",8]), optimize=TRUE)$lambda
  # print(box.cox)
  # temp_data <- as.double(cur_seasondf[cur_seasondf$Name=="Alexander Bolshunov",8])
  # temp_data <- temp_data^box.cox
  
  #print(pnorm(1^box.cox, mean=mean(temp_data), sd=sd(temp_data)))
  #print(shapiro.test(temp_places))
  # name_list <- unique(cur_seasondf$Name)
  # name_list.probs <- table(name_pot)/length(name_pot)
  # name_sample <- sample(x=name_list, size=simnum_racers, replace=FALSE, prob=as.numeric(as.matrix(name_list.probs)))
  # print(name_sample[order(name_sample)])
  simmendf <- simmendf[-which(simmendf$Seasons==as.character(a)), ]
  simmendf <- rbind(simmendf, cur_seasondf)
}

for(a in 1982:2020){
  print(a)
  lcur_seasondf <- simladiesdf[simladiesdf$Seasons==as.character(a), ]
  #
  lnum_racers <- as.numeric(as.matrix(table(lcur_seasondf$Date)))
  lnum_races <- length(unique(lcur_seasondf$Date))
  #print(lnum_races)
  lnum_simraces <- 38-lnum_races
  

  
  
  # print(table(name_lpot))
  sim_lresults <- c()
  for(z in 1: lnum_simraces){
    name_lpot <- lcur_seasondf$Name
    # print(table(name_lpot))
    name_lpot_del <- names(table(name_lpot)[which(as.numeric(as.matrix(table(name_lpot)))<2)])
    #print(name_lpot_del)
    name_lpot = name_lpot[! name_lpot %in% name_lpot_del]
    #print(length(unique(name_lpot)))
    
    if(max(lnum_racers)+1 > length(unique(name_lpot))){
      simlnum_racers <- as.integer(rtruncnorm(1, min(lnum_racers), length(unique(name_lpot)), mean(lnum_racers), sd(lnum_racers)))
    }
    else{
      simlnum_racers <- as.integer(rtruncnorm(1, min(lnum_racers), max(lnum_racers)+1, mean(lnum_racers), sd(lnum_racers)))
    }
    #print(simlnum_racers)
    
    
    
    
    lname_sample <- c()
    for(b in 1:simlnum_racers){
      lsim_racer <- sample(name_lpot, 1)
      
      #print(lsim_racer)
      lname_sample <- append(lname_sample, lsim_racer)
      name_lpot <- name_lpot[! name_lpot %in% lsim_racer]
      #setdiff[name_lpot, lsim_racer]
    }
    #print(lname_sample)
    #print(lname_sample)
    
    (lcur_seasondf[lcur_seasondf$Name=="Sung-Hoon Cho", ])
    #lcur_seasondf[which(lcur_seasondf$Name %in% lname_sample), ]
    lname_sample <- (lname_sample[order(lname_sample)])
    # print(lname_sample)
    pnorm(1, mean=mean(as.double(lcur_seasondf[lname_sample,8])), 
          sd=sd(as.double(lcur_seasondf[lname_sample,8])))
    
    lresults = c()
    sim_lnation <- c()
    for(c in 1:(simlnum_racers)){
      lpos_prob_tot=0
      lraw_pnormss = c()
      
      for(d in 1:length(lname_sample)){
        
        lraw_pnorms <- pnorm(c, mean=mean(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[d],8])), 
                             sd=sd(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[d],8])))
        # print(lname_sample[d])
        # print(lraw_pnorms)
        # print(lraw_pnorms)
        lraw_pnormss <- append(lraw_pnormss, lraw_pnorms)
        lpos_prob_tot <- lpos_prob_tot +pnorm(c, mean=mean(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[d],8])), 
                                              sd=sd(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[d],8])))
        #print(lname_sample[c])
        #print(pnorm(1, mean=mean(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[c],8])), 
        #sd=sd(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[c],8]))))
        
      }
      lasj_pnorms <- lraw_pnormss/lpos_prob_tot
      if(is.na(lasj_pnorms[length(lasj_pnorms)])){
        lpos <- lname_sample[length(lasj_pnorms)]
      }
      else{
        lpos <- sample(lname_sample, 1, prob=lasj_pnorms)
      }
      sim_lnation = (append(sim_lnation, lcur_seasondf[which(lcur_seasondf$Name==lpos)[1], 10]))
      # print(lcur_seasondf[which(lcur_seasondf$Name==lpos)[1], 10])
      #print(lname_sample)
      #print(lpos)
      lname_sample <- lname_sample[! lname_sample %in% lpos]
      lresults = append(lresults, lpos)
      #print(lresults)
    }
    lresults_names <- lresults
    lresults_lpos <- 1:length(lresults_names)
    #print(lresults_names)
    lresults_city <- rep("Sim City", length(lresults_names))
    lresults_country <- rep("Simland", length(lresults_names))
    lresults_event <- rep("Sim", length(lresults_names))
    lresults_sex <- rep("M", length(lresults_names))
    lresults_distance <- rep("Sim", length(lresults_names))
    lresults_discipline <- rep("Sim", length(lresults_names))
    lresults_date <- rep(as.character(as.double(lcur_seasondf[length(lcur_seasondf$Date), 1])+1), length(lresults_names))
    lresults_season <- rep(a, length(lresults_names))
    
    
    sim_lresults<-data.frame(as.character(lresults_date), as.character(lresults_city), as.character(lresults_country), as.character(lresults_event), as.character(lresults_sex), 
                             as.character(lresults_distance), as.character(lresults_discipline), as.character(lresults_lpos), as.character(lresults_names),
                             as.character(sim_lnation), as.character(lresults_season))
    
    names(sim_lresults) <- names(lcur_seasondf)
    #print(sim_lresults)
    
    lcur_seasondf <- rbind(lcur_seasondf, sim_lresults)
    #sim_lresults = append(sim_lresults, lresults)
  }
  #Box cox if you need it later
  # box.cox <- boxcox(as.double(lcur_seasondf[lcur_seasondf$Name=="Johannes Høsflot Klæbo",8]), optimize=TRUE)$lambda
  # print(box.cox)
  # temp_data <- as.double(lcur_seasondf[lcur_seasondf$Name=="Alexander Bolshunov",8])
  # temp_data <- temp_data^box.cox
  
  #print(pnorm(1^box.cox, mean=mean(temp_data), sd=sd(temp_data)))
  #print(shapiro.test(temp_places))
  # name_list <- unique(lcur_seasondf$Name)
  # name_list.probs <- table(name_lpot)/length(name_lpot)
  # lname_sample <- sample(x=name_list, size=simlnum_racers, replace=FALSE, prob=as.numeric(as.matrix(name_list.probs)))
  # print(lname_sample[order(lname_sample)])
  simladiesdf <- simladiesdf[-which(simladiesdf$Seasons==as.character(a)), ]
  simladiesdf <- rbind(simladiesdf, lcur_seasondf)
}
# 
# print(pnorm(as.double(cur_seasondf[cur_seasondf$Name=="Johannes Høsflot Klæbo",8]), mean=mean(as.double(cur_seasondf[cur_seasondf$Name=="Johannes Høsflot Klæbo",8])), 
#             sd=sd(as.double(cur_seasondf[cur_seasondf$Name=="Johannes Høsflot Klæbo",8]))))
# 
# 
# print(dnorm(1, mean=mean(as.double(cur_seasondf[cur_seasondf$Name=="Simen Hegstad Krüger",8])), 
#             sd=sd(as.double(cur_seasondf[cur_seasondf$Name=="Simen Hegstad Krüger",8]))))
# 
# print(pnorm(as.double(cur_seasondf[cur_seasondf$Name=="Alexander Bolshunov",8]), mean=mean(as.double(cur_seasondf[cur_seasondf$Name=="Alexander Bolshunov",8])), 
#             sd=sd(as.double(cur_seasondf[cur_seasondf$Name=="Alexander Bolshunov",8]))))
# skew(as.double(cur_seasondf[cur_seasondf$Name=="Johannes Høsflot Klæbo",8]))
# skew(as.double(cur_seasondf[cur_seasondf$Name=="Alexander Bolshunov",8]))
# skew(as.double(cur_seasondf[cur_seasondf$Name=="Simen Hegstad Krüger",8]))
# shapiro.test(as.double(cur_seasondf[cur_seasondf$Name=="Sjur Røthe",8]))
simmaleraces = list()
simladyraces = list()

for(a in 1:length(simmendf$Seasons)){
  if(simmendf$Seasons[a] %in% names(simmaleraces)==FALSE){
    simmaleraces[[simmendf$Seasons[a]]] <- list()
  }
  
  if(simmendf$Date[a] %in% names(simmaleraces[[simmendf$Seasons[a]]])==FALSE){
    simmaleraces[[simmendf$Seasons[a]]][[simmendf$Date[a]]] <- list()
  }
  if(simmendf$Distance[a] %in% names(simmaleraces[[simmendf$Seasons[a]]][[simmendf$Date[a]]])==FALSE){
    simmaleraces[[simmendf$Seasons[a]]][[simmendf$Date[a]]][[simmendf$Distance[a]]]<-list()
  }
  if(simmendf$Name[a] %in% names(simmaleraces[[simmendf$Seasons[a]]][[simmendf$Date[a]]][[simmendf$Distance[a]]])==FALSE){
    simmaleraces[[simmendf$Seasons[a]]][[simmendf$Date[a]]][[simmendf$Distance[a]]][[simmendf$Name[a]]]<-as.double(simmendf$Place[a])
  }
}

for(a in 1:length(simladiesdf$Seasons)){
  if(simladiesdf$Seasons[a] %in% names(simladyraces)==FALSE){
    simladyraces[[simladiesdf$Seasons[a]]] <- list()
  }
  
  if(simladiesdf$Date[a] %in% names(simladyraces[[simladiesdf$Seasons[a]]])==FALSE){
    simladyraces[[simladiesdf$Seasons[a]]][[simladiesdf$Date[a]]] <- list()
  }
  if(simladiesdf$Distance[a] %in% names(simladyraces[[simladiesdf$Seasons[a]]][[simladiesdf$Date[a]]])==FALSE){
    simladyraces[[simladiesdf$Seasons[a]]][[simladiesdf$Date[a]]][[simladiesdf$Distance[a]]]<-list()
  }
  if(simladiesdf$Name[a] %in% names(simladyraces[[simladiesdf$Seasons[a]]][[simladiesdf$Date[a]]][[simladiesdf$Distance[a]]])==FALSE){
    simladyraces[[simladiesdf$Seasons[a]]][[simladiesdf$Date[a]]][[simladiesdf$Distance[a]]][[simladiesdf$Name[a]]]<-as.double(simladiesdf$Place[a])
  }
}





savesimmaleraces<-simmaleraces
savesimladyraces <- simladyraces

#Initialize all skiers to have an Elo of 1300
simmaleelo = list()
simladyelo = list()
for(a in 1:length(simmendf$Name)){
  if(simmendf$Name[a] %in% names(simmaleelo) == FALSE){
    simmaleelo[[simmendf$Name[a]]] <- list()
    simmaleelo[[simmendf$Name[a]]][["0000"]]<-list()
    simmaleelo[[simmendf$Name[a]]][["0000"]][["00000000"]]<-list()
    simmaleelo[[simmendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(simladiesdf$Name)){
  if(simladiesdf$Name[a] %in% names(simladyelo) == FALSE){
    simladyelo[[simladiesdf$Name[a]]] <- list()
    simladyelo[[simladiesdf$Name[a]]][["0000"]]<-list()
    simladyelo[[simladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    simladyelo[[simladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savesimmaleelo<-simmaleelo
savesimladyelo <- simladyelo

#first attempt at an elo rating
simmaleelo<-savesimmaleelo

alsimlelo <- c()
K=1
place_index = 1

for(z in 1:length(simmaleraces)){
  print(z)
  
  for(a in 1:length(simmaleraces[[z]])){
    
    
    for(b in 1:length(simmaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(simmaleraces[[z]][[a]][[b]])){
        lastp = simmaleelo[[names(simmaleraces[[z]][[a]][[b]][c])]][[length(simmaleelo[[names(simmaleraces[[z]][[a]][[b]][c])]])]][[length(simmaleelo[[names(simmaleraces[[z]][[a]][[b]][c])]][[length(simmaleelo[[names(simmaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(simmaleraces[[z]][[a]][[b]]), as.double(simmendf$Place[place_index:(place_index+length(names(simmaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(simmaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(simmaleraces[[z]][[a]][[b]][c])
        current_year <- names(simmaleraces[z])
        if(current_year %in% names(simmaleelo[[names(simmaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          simmaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(simmaleraces[[z]][a])
        if(current_date %in% names(simmaleelo[[names(simmaleraces[[z]][[a]][[b]][c])]][[names(simmaleraces[z])]])==FALSE){
          simmaleelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(simmaleraces[[z]][[a]][b])
        if(current_race %in% names(simmaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          simmaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        alsimlelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(simmaleelo)){
    if(length(simmaleelo[[d]])>1){
      plastp = simmaleelo[[d]][[length(simmaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #simmaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      simmaleelo[[d]][[current_year]][[current_date]] <- list()
      simmaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
simladyelo <- savesimladyelo
place_index = 1
alsimlelo <- c()
for(z in 1:length(simladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(simladyraces[[z]])){
    
    
    for(b in 1:length(simladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(simladyraces[[z]][[a]][[b]])){
        lastp = simladyelo[[names(simladyraces[[z]][[a]][[b]][c])]][[length(simladyelo[[names(simladyraces[[z]][[a]][[b]][c])]])]][[length(simladyelo[[names(simladyraces[[z]][[a]][[b]][c])]][[length(simladyelo[[names(simladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(simladyraces[[z]][[a]][[b]]), as.double(simladiesdf$Place[place_index:(place_index+length(names(simladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(simladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(simladyraces[[z]][[a]][[b]][c])
        current_year <- names(simladyraces[z])
        if(current_year %in% names(simladyelo[[names(simladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          simladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(simladyraces[[z]][a])
        if(current_date %in% names(simladyelo[[names(simladyraces[[z]][[a]][[b]][c])]][[names(simladyraces[z])]])==FALSE){
          simladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(simladyraces[[z]][[a]][b])
        if(current_race %in% names(simladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          simladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        alsimlelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(simladyelo)){
    if(length(simladyelo[[d]])>1){
      plastp = simladyelo[[d]][[length(simladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #simladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      simladyelo[[d]][[current_year]][[current_date]] <- list()
      simladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}

#Now time to make a data frame

#Name, Season, Date, Distance, Elo
simmaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

simmnames = c()
blseason = c()
simmdate = c()
simmdistance = c()
simmelo = c()
for (a in 1:length(simmaleelo)){
  tick=0
  for(b in 1:length(simmaleelo[[a]])){
    for(c in 1:length(simmaleelo[[a]][[b]])){
      for(d in 1:length(simmaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #simmnames2 = append(simmnames2,as.character(names(simmaleelo[a])))
      }
    }
  }
  simmnames = append(simmnames, rep(as.character(names(simmaleelo[a])), tick))
}


for (a in 1:length(simmaleelo)){
  for(b in 1:length(simmaleelo[[a]])){
    for(c in 1:length(simmaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(simmaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      simmdate=append(simmdate, rep(as.character(names(simmaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(simmaleelo)){
  for(b in 1:length(simmaleelo[[a]])){
    for(c in 1:length(simmaleelo[[a]][[b]])){
      for(d in 1:length(simmaleelo[[a]][[b]][[c]])){
        simmdistance=append(simmdistance, as.character(names(simmaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(simmaleelo)){
  for(b in 1:length(simmaleelo[[a]])){
    for(c in 1:length(simmaleelo[[a]][[b]])){
      for(d in 1:length(simmaleelo[[a]][[b]][[c]])){
        simmelo=append(simmelo, as.double(simmaleelo[[a]][[b]][[c]][[d]]))}}}}


simmaleelodf <- data.frame(simmnames, simmdate, simmdistance, simmelo)



simladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

simlnames = c()
blseason = c()
simldate = c()
simldistance = c()
simlelo = c()
for (a in 1:length(simladyelo)){
  tick=0
  for(b in 1:length(simladyelo[[a]])){
    for(c in 1:length(simladyelo[[a]][[b]])){
      for(d in 1:length(simladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #simlnames2 = append(simlnames2,as.character(names(simladyelo[a])))
      }
    }
  }
  simlnames = append(simlnames, rep(as.character(names(simladyelo[a])), tick))
}


for (a in 1:length(simladyelo)){
  for(b in 1:length(simladyelo[[a]])){
    for(c in 1:length(simladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(simladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      simldate=append(simldate, rep(as.character(names(simladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(simladyelo)){
  for(b in 1:length(simladyelo[[a]])){
    for(c in 1:length(simladyelo[[a]][[b]])){
      for(d in 1:length(simladyelo[[a]][[b]][[c]])){
        simldistance=append(simldistance, as.character(names(simladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(simladyelo)){
  for(b in 1:length(simladyelo[[a]])){
    for(c in 1:length(simladyelo[[a]][[b]])){
      for(d in 1:length(simladyelo[[a]][[b]][[c]])){
        simlelo=append(simlelo, as.double(simladyelo[[a]][[b]][[c]][[d]]))}}}}


simladyelodf <- data.frame(simlnames, simldate, simldistance, simlelo)

savesimmnames <- simmnames
savesimmdistance <- simmdistance
savesimmdate <- simmdate
savesimmelo <- simmelo
savesimlnames <- simlnames
savesimldistance <- simldistance
savesimldate <- simldate
savesimlelo <- simlelo


simmalehighest <- simmaleelodf[order(-simmaleelodf$simmelo), ]
simmalehighest_ind <- simmalehighest[match(unique(simmalehighest$simmnames), simmalehighest$simmnames), ]
row.names(simmalehighest_ind) <- 1:length(simmalehighest_ind[,1])
simmalehighest_ind[1:25,]


simladyhighest <- simladyelodf[order(-simladyelodf$simlelo), ]
simladyhighest_ind <-simladyhighest[match(unique(simladyhighest$simlnames), simladyhighest$simlnames), ]
row.names(simladyhighest_ind) <- 1:length(simladyhighest_ind[,1])
simladyhighest_ind[1:25, ]

simmlast_race <- simmaleelodf[simmaleelodf$simmdate=="20210500",]
simmlast_race[order(-mlast_race$simmelo),]

library(ggplot2)

jhk = simmaleelodf[(simmaleelodf$simmnames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = simmaleelodf[(simmaleelodf$simmnames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = simmaleelodf[(simmaleelodf$simmnames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = simmaleelodf[(simmaleelodf$simmnames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = simmaleelodf[(simmaleelodf$simmnames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = simmaleelodf[(simmaleelodf$simmnames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(simmelo), colour=simmnames)) +geom_point()


bd = simmaleelodf[(simmaleelodf$simmnames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = simmaleelodf[(simmaleelodf$simmnames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = simmaleelodf[(simmaleelodf$simmnames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = simmaleelodf[(simmaleelodf$simmnames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = simmaleelodf[(simmaleelodf$simmnames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = simmaleelodf[(simmaleelodf$simmnames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = simmaleelodf[(simmaleelodf$simmnames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = simmaleelodf[(simmaleelodf$simmnames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(simmelo), colour=simmnames)) +geom_point()

top <- rbind(bd, jhk, vs,bolsh,pnj,ta)
ggplot(top, aes(race, as.double(simmelo), colour=simmnames)) +geom_point()
simmaleelodf[which(simmaleelodf$simmnames=="Kevin Brochman"),]

jd = simladyelodf[(simladyelodf$simlnames)=="Jessica Diggins", ]
kr = simladyelodf[(simladyelodf$simlnames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(simlelo), colour=simlnames)) +geom_point()

tj = simladyelodf[(simladyelodf$simlnames)=="Therese Johaug", ]
mb = simladyelodf[(simladyelodf$simlnames)=="Marit Bjørgen", ]
io = simladyelodf[(simladyelodf$simlnames)=="Ingvild Flugstad Østberg", ]
hw = simladyelodf[(simladyelodf$simlnames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(simlelo), colour=simlnames)) +geom_point()


simladyelodf[(simladyelodf$simlnames)=="Rosie Brennan", ]


simmlast_race <- simmaleelodf[simmaleelodf$simmdate=="20210500",]
simmlast_race <- simmlast_race[order(-simmlast_race$simmelo),]
row.names(simmlast_race) <- 1:length(simmlast_race[,1])
simmlast_race[1:25,]

simllast_race <- simladyelodf[simladyelodf$simldate=="20210500", ]
simllast_race <- simllast_race[order(-simllast_race$simlelo),]
row.names(simllast_race) <- 1:length(simllast_race[,1])
simllast_race[1:25,]


