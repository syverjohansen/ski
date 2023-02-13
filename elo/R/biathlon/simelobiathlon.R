library("tidyverse")
library(simstudy)
library(psych)
library(SuppDists)
library(truncdist)
library(truncnorm)
library(EnvStats)
library(e1071)
bsimmen <- read_excel("~/ski/elo/biathlon/excel365/all.xlsx", 
                     sheet = "Men", col_names = FALSE, na = "NA")
bsimladies <- read_excel("~/ski/elo/biathlon/excel365/all.xlsx", 
                        sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
bsimmendf <- data.frame(bsimmen)
names(bsimmendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
bsimladiesdf <- data.frame(bsimladies)
names(bsimladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")

bsimmendf[which(bsimmendf$Name=="Ricco Gross"), ]
bsimmendf[9757,9] = "Ricco Gross2"
bsimmendf[9698,9] = "Ricco Gross2"
bsimmendf[9365,9] = "Uros Velepec2"
bsimmendf[9360, 9] = "Petr Garabik2"
bsimmendf[9139,9] = "Alexandr Popov2"
bsimmendf[392,9] = "Sten Eriksson2"
bsimmendf[which(bsimmendf$Distance=="Stage"), 1] <- as.character(as.double(bsimmendf[which(bsimmendf$Distance=="Stage"), 1])+1)
bsimladiesdf[which(bsimladiesdf$Distance=="Stage"), 1] <- as.character(as.double(bsimladiesdf[which(bsimladiesdf$Distance=="Stage"), 1])+1)



bsimmendf$Seasons = NA
bsimladiesdf$Seasons = NA
for(a in 1:length(bsimmendf$Date)){
  if((as.double(substr(bsimmendf$Date[a], 5, 8)) > 1000) && as.double(substr(bsimmendf$Date[a],5,8)!=9999)){
    bsimmendf$Seasons[a] = as.character(as.double(substr(bsimmendf$Date[a], 1, 4))+1)
  }
  else{
    bsimmendf$Seasons[a] = as.character(as.double(substr(bsimmendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(bsimladiesdf$Date)){
  if((as.double(substr(bsimladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(bsimladiesdf$Date[a],5,8)!=9999)){
    bsimladiesdf$Seasons[a] = as.character(as.double(substr(bsimladiesdf$Date[a], 1, 4))+1)
  }
  else{
    bsimladiesdf$Seasons[a] = as.character(as.double(substr(bsimladiesdf$Date[a], 1, 4)))
  }
}

#bsimladiesdf[24331, 9] <- "Tatjana Kuznetsova2"

for(a in 1978:2020){
  print(a)
  bcur_seasondf <- bsimmendf[bsimmendf$Seasons==as.character(a), ]
  #
  bnum_racers <- as.numeric(as.matrix(table(bcur_seasondf$Date)))
  bnum_races <- length(unique(bcur_seasondf$Date))
  #print(bnum_races)
  num_bsimraces <- 27-bnum_races

  
  # print(table(bname_pot))
  bsim_bresults <- c()
  for(z in 1: num_bsimraces){
    bname_pot <- bcur_seasondf$Name
    # print(table(bname_pot))
    bname_pot_del <- names(table(bname_pot)[which(as.numeric(as.matrix(table(bname_pot)))<2)])
    #print(bname_pot_del)
    bname_pot = bname_pot[! bname_pot %in% bname_pot_del]
    #print(length(unique(bname_pot)))
    
    if(max(bnum_racers)+1 > length(unique(bname_pot))){
      bsimbnum_racers <- as.integer(rtruncnorm(1, min(bnum_racers), length(unique(bname_pot)), mean(bnum_racers), sd(bnum_racers)))
    }
    else{
      bsimbnum_racers <- as.integer(rtruncnorm(1, min(bnum_racers), max(bnum_racers)+1, mean(bnum_racers), sd(bnum_racers)))
    }
    #print(bsimbnum_racers)
    
    
    
    
    bname_sample <- c()
    for(b in 1:bsimbnum_racers){
      bsim_racer <- sample(bname_pot, 1)
      
      #print(bsim_racer)
      bname_sample <- append(bname_sample, bsim_racer)
      bname_pot <- bname_pot[! bname_pot %in% bsim_racer]
      #setdiff[bname_pot, bsim_racer]
    }
    #print(bname_sample)
    #print(bname_sample)
    
    (bcur_seasondf[bcur_seasondf$Name=="Sung-Hoon Cho", ])
    #bcur_seasondf[which(bcur_seasondf$Name %in% bname_sample), ]
    bname_sample <- (bname_sample[order(bname_sample)])
    # print(bname_sample)
    pnorm(1, mean=mean(as.double(bcur_seasondf[bname_sample,8])), 
          sd=sd(as.double(bcur_seasondf[bname_sample,8])))
    
    bresults = c()
    bsim_nation <- c()
    for(c in 1:(bsimbnum_racers)){
      bpos_prob_tot=0
      braw_pnorms = c()
      
      for(d in 1:length(bname_sample)){
        
        braw_pnorm <- pnorm(c, mean=mean(as.double(bcur_seasondf[bcur_seasondf$Name==bname_sample[d],8])), 
                           sd=sd(as.double(bcur_seasondf[bcur_seasondf$Name==bname_sample[d],8])))
        # print(bname_sample[d])
        # print(braw_pnorm)
        # print(braw_pnorm)
        braw_pnorms <- append(braw_pnorms, braw_pnorm)
        bpos_prob_tot <- bpos_prob_tot +pnorm(c, mean=mean(as.double(bcur_seasondf[bcur_seasondf$Name==bname_sample[d],8])), 
                                            sd=sd(as.double(bcur_seasondf[bcur_seasondf$Name==bname_sample[d],8])))
        #print(bname_sample[c])
        #print(pnorm(1, mean=mean(as.double(bcur_seasondf[bcur_seasondf$Name==bname_sample[c],8])), 
        #sd=sd(as.double(bcur_seasondf[bcur_seasondf$Name==bname_sample[c],8]))))
        
      }
      badj_pnorms <- braw_pnorms/bpos_prob_tot
      if(is.na(badj_pnorms[length(badj_pnorms)])){
        bpos <- bname_sample[length(badj_pnorms)]
      }
      else{
        bpos <- sample(bname_sample, 1, prob=badj_pnorms)
      }
      bsim_nation = (append(bsim_nation, bcur_seasondf[which(bcur_seasondf$Name==bpos)[1], 10]))
      # print(bcur_seasondf[which(bcur_seasondf$Name==bpos)[1], 10])
      #print(bname_sample)
      #print(bpos)
      bname_sample <- bname_sample[! bname_sample %in% bpos]
      bresults = append(bresults, bpos)
      #print(bresults)
    }
    bresults_names <- bresults
    bresults_bpos <- 1:length(bresults_names)
    #print(bresults_names)
    bresults_city <- rep("Sim City", length(bresults_names))
    bresults_country <- rep("Simland", length(bresults_names))
    bresults_event <- rep("Sim", length(bresults_names))
    bresults_sex <- rep("M", length(bresults_names))
    bresults_distance <- rep("Sim", length(bresults_names))
    bresults_discipline <- rep("Sim", length(bresults_names))
    bresults_date <- rep(as.character(as.double(bcur_seasondf[length(bcur_seasondf$Date), 1])+1), length(bresults_names))
    bresults_season <- rep(a, length(bresults_names))
    
    
    bsim_bresults<-data.frame(as.character(bresults_date), as.character(bresults_city), as.character(bresults_country), as.character(bresults_event), as.character(bresults_sex), 
                            as.character(bresults_distance), as.character(bresults_discipline), as.character(bresults_bpos), as.character(bresults_names),
                            as.character(bsim_nation), as.character(bresults_season))
    
    names(bsim_bresults) <- names(bcur_seasondf)
    #print(bsim_bresults)
    
    bcur_seasondf <- rbind(bcur_seasondf, bsim_bresults)
    #bsim_bresults = append(bsim_bresults, bresults)
  }
  #Box cox if you need it later
  # box.cox <- boxcox(as.double(bcur_seasondf[bcur_seasondf$Name=="Johannes Høsflot Klæbo",8]), optimize=TRUE)$lambda
  # print(box.cox)
  # temp_data <- as.double(bcur_seasondf[bcur_seasondf$Name=="Alexander Bolshunov",8])
  # temp_data <- temp_data^box.cox
  
  #print(pnorm(1^box.cox, mean=mean(temp_data), sd=sd(temp_data)))
  #print(shapiro.test(temp_places))
  # name_list <- unique(bcur_seasondf$Name)
  # name_list.probs <- table(bname_pot)/length(bname_pot)
  # bname_sample <- sample(x=name_list, size=bsimbnum_racers, replace=FALSE, prob=as.numeric(as.matrix(name_list.probs)))
  # print(bname_sample[order(bname_sample)])
  bsimmendf <- bsimmendf[-which(bsimmendf$Seasons==as.character(a)), ]
  bsimmendf <- rbind(bsimmendf, bcur_seasondf)
}

for(a in 1988:2020){
  print(a)
  lbcur_seasondf <- bsimladiesdf[bsimladiesdf$Seasons==as.character(a), ]
  #
  lbnum_racers <- as.numeric(as.matrix(table(lbcur_seasondf$Date)))
  lbnum_races <- length(unique(lbcur_seasondf$Date))
  #print(lbnum_races)
  lnum_bsimraces <- 27-lbnum_races

  
  
  
  # print(table(banme_lpot))
  bsim_lbresults <- c()
  for(z in 1: lnum_bsimraces){
    banme_lpot <- lbcur_seasondf$Name
    # print(table(banme_lpot))
    banme_lpot_del <- names(table(banme_lpot)[which(as.numeric(as.matrix(table(banme_lpot)))<2)])
    #print(banme_lpot_del)
    banme_lpot = banme_lpot[! banme_lpot %in% banme_lpot_del]
    #print(length(unique(banme_lpot)))
    
    if(max(lbnum_racers)+1 > length(unique(banme_lpot))){
      bsimlbnum_racers <- as.integer(rtruncnorm(1, min(lbnum_racers), length(unique(banme_lpot)), mean(lbnum_racers), sd(lbnum_racers)))
    }
    else{
      bsimlbnum_racers <- as.integer(rtruncnorm(1, min(lbnum_racers), max(lbnum_racers)+1, mean(lbnum_racers), sd(lbnum_racers)))
    }
    #print(bsimlbnum_racers)
    
    
    
    
    lbname_sample <- c()
    for(b in 1:bsimlbnum_racers){
      lbsim_racer <- sample(banme_lpot, 1)
      
      #print(lbsim_racer)
      lbname_sample <- append(lbname_sample, lbsim_racer)
      banme_lpot <- banme_lpot[! banme_lpot %in% lbsim_racer]
      #setdiff[banme_lpot, lbsim_racer]
    }
    #print(lbname_sample)
    #print(lbname_sample)
    
    (lbcur_seasondf[lbcur_seasondf$Name=="Sung-Hoon Cho", ])
    #lbcur_seasondf[which(lbcur_seasondf$Name %in% lbname_sample), ]
    lbname_sample <- (lbname_sample[order(lbname_sample)])
    # print(lbname_sample)
    pnorm(1, mean=mean(as.double(lbcur_seasondf[lbname_sample,8])), 
          sd=sd(as.double(lbcur_seasondf[lbname_sample,8])))
    
    lbresults = c()
    bsim_lnation <- c()
    for(c in 1:(bsimlbnum_racers)){
      lbpos_prob_tot=0
      lbraw_pnormss = c()
      
      for(d in 1:length(lbname_sample)){
        
        lbraw_pnorms <- pnorm(c, mean=mean(as.double(lbcur_seasondf[lbcur_seasondf$Name==lbname_sample[d],8])), 
                             sd=sd(as.double(lbcur_seasondf[lbcur_seasondf$Name==lbname_sample[d],8])))
        # print(lbname_sample[d])
        # print(lbraw_pnorms)
        # print(lbraw_pnorms)
        lbraw_pnormss <- append(lbraw_pnormss, lbraw_pnorms)
        lbpos_prob_tot <- lbpos_prob_tot +pnorm(c, mean=mean(as.double(lbcur_seasondf[lbcur_seasondf$Name==lbname_sample[d],8])), 
                                              sd=sd(as.double(lbcur_seasondf[lbcur_seasondf$Name==lbname_sample[d],8])))
        #print(lbname_sample[c])
        #print(pnorm(1, mean=mean(as.double(lbcur_seasondf[lbcur_seasondf$Name==lbname_sample[c],8])), 
        #sd=sd(as.double(lbcur_seasondf[lbcur_seasondf$Name==lbname_sample[c],8]))))
        
      }
      blasj_pnorms <- lbraw_pnormss/lbpos_prob_tot
      if(is.na(blasj_pnorms[length(blasj_pnorms)])){
        lbpos <- lbname_sample[length(blasj_pnorms)]
      }
      else{
        lbpos <- sample(lbname_sample, 1, prob=blasj_pnorms)
      }
      bsim_lnation = (append(bsim_lnation, lbcur_seasondf[which(lbcur_seasondf$Name==lbpos)[1], 10]))
      # print(lbcur_seasondf[which(lbcur_seasondf$Name==lbpos)[1], 10])
      #print(lbname_sample)
      #print(lbpos)
      lbname_sample <- lbname_sample[! lbname_sample %in% lbpos]
      lbresults = append(lbresults, lbpos)
      #print(lbresults)
    }
    lbresults_names <- lbresults
    lbresults_lbpos <- 1:length(lbresults_names)
    #print(lbresults_names)
    lbresults_city <- rep("Sim City", length(lbresults_names))
    lbresults_country <- rep("Simland", length(lbresults_names))
    lbresults_event <- rep("Sim", length(lbresults_names))
    lbresults_sex <- rep("M", length(lbresults_names))
    lbresults_distance <- rep("Sim", length(lbresults_names))
    lbresults_discipline <- rep("Sim", length(lbresults_names))
    lbresults_date <- rep(as.character(as.double(lbcur_seasondf[length(lbcur_seasondf$Date), 1])+1), length(lbresults_names))
    lbresults_season <- rep(a, length(lbresults_names))
    
    
    bsim_lbresults<-data.frame(as.character(lbresults_date), as.character(lbresults_city), as.character(lbresults_country), as.character(lbresults_event), as.character(lbresults_sex), 
                             as.character(lbresults_distance), as.character(lbresults_discipline), as.character(lbresults_lbpos), as.character(lbresults_names),
                             as.character(bsim_lnation), as.character(lbresults_season))
    
    names(bsim_lbresults) <- names(lbcur_seasondf)
    #print(bsim_lbresults)
    
    lbcur_seasondf <- rbind(lbcur_seasondf, bsim_lbresults)
    #bsim_lbresults = append(bsim_lbresults, lbresults)
  }
  #Box cox if you need it later
  # box.cox <- boxcox(as.double(lbcur_seasondf[lbcur_seasondf$Name=="Johannes Høsflot Klæbo",8]), optimize=TRUE)$lambda
  # print(box.cox)
  # temp_data <- as.double(lbcur_seasondf[lbcur_seasondf$Name=="Alexander Bolshunov",8])
  # temp_data <- temp_data^box.cox
  
  #print(pnorm(1^box.cox, mean=mean(temp_data), sd=sd(temp_data)))
  #print(shapiro.test(temp_places))
  # name_list <- unique(lbcur_seasondf$Name)
  # name_list.probs <- table(banme_lpot)/length(banme_lpot)
  # lbname_sample <- sample(x=name_list, size=bsimlbnum_racers, replace=FALSE, prob=as.numeric(as.matrix(name_list.probs)))
  # print(lbname_sample[order(lbname_sample)])
  bsimladiesdf <- bsimladiesdf[-which(bsimladiesdf$Seasons==as.character(a)), ]
  bsimladiesdf <- rbind(bsimladiesdf, lbcur_seasondf)
}
# 
# print(pnorm(as.double(bcur_seasondf[bcur_seasondf$Name=="Johannes Høsflot Klæbo",8]), mean=mean(as.double(bcur_seasondf[bcur_seasondf$Name=="Johannes Høsflot Klæbo",8])), 
#             sd=sd(as.double(bcur_seasondf[bcur_seasondf$Name=="Johannes Høsflot Klæbo",8]))))
# 
# 
# print(dnorm(1, mean=mean(as.double(bcur_seasondf[bcur_seasondf$Name=="Simen Hegstad Krüger",8])), 
#             sd=sd(as.double(bcur_seasondf[bcur_seasondf$Name=="Simen Hegstad Krüger",8]))))
# 
# print(pnorm(as.double(bcur_seasondf[bcur_seasondf$Name=="Alexander Bolshunov",8]), mean=mean(as.double(bcur_seasondf[bcur_seasondf$Name=="Alexander Bolshunov",8])), 
#             sd=sd(as.double(bcur_seasondf[bcur_seasondf$Name=="Alexander Bolshunov",8]))))
# skew(as.double(bcur_seasondf[bcur_seasondf$Name=="Johannes Høsflot Klæbo",8]))
# skew(as.double(bcur_seasondf[bcur_seasondf$Name=="Alexander Bolshunov",8]))
# skew(as.double(bcur_seasondf[bcur_seasondf$Name=="Simen Hegstad Krüger",8]))
# shapiro.test(as.double(bcur_seasondf[bcur_seasondf$Name=="Sjur Røthe",8]))
bsimmaleraces = list()
bsimladyraces = list()

for(a in 1:length(bsimmendf$Seasons)){
  if(bsimmendf$Seasons[a] %in% names(bsimmaleraces)==FALSE){
    bsimmaleraces[[bsimmendf$Seasons[a]]] <- list()
  }
  
  if(bsimmendf$Date[a] %in% names(bsimmaleraces[[bsimmendf$Seasons[a]]])==FALSE){
    bsimmaleraces[[bsimmendf$Seasons[a]]][[bsimmendf$Date[a]]] <- list()
  }
  if(bsimmendf$Distance[a] %in% names(bsimmaleraces[[bsimmendf$Seasons[a]]][[bsimmendf$Date[a]]])==FALSE){
    bsimmaleraces[[bsimmendf$Seasons[a]]][[bsimmendf$Date[a]]][[bsimmendf$Distance[a]]]<-list()
  }
  if(bsimmendf$Name[a] %in% names(bsimmaleraces[[bsimmendf$Seasons[a]]][[bsimmendf$Date[a]]][[bsimmendf$Distance[a]]])==FALSE){
    bsimmaleraces[[bsimmendf$Seasons[a]]][[bsimmendf$Date[a]]][[bsimmendf$Distance[a]]][[bsimmendf$Name[a]]]<-as.double(bsimmendf$Place[a])
  }
}

for(a in 1:length(bsimladiesdf$Seasons)){
  if(bsimladiesdf$Seasons[a] %in% names(bsimladyraces)==FALSE){
    bsimladyraces[[bsimladiesdf$Seasons[a]]] <- list()
  }
  
  if(bsimladiesdf$Date[a] %in% names(bsimladyraces[[bsimladiesdf$Seasons[a]]])==FALSE){
    bsimladyraces[[bsimladiesdf$Seasons[a]]][[bsimladiesdf$Date[a]]] <- list()
  }
  if(bsimladiesdf$Distance[a] %in% names(bsimladyraces[[bsimladiesdf$Seasons[a]]][[bsimladiesdf$Date[a]]])==FALSE){
    bsimladyraces[[bsimladiesdf$Seasons[a]]][[bsimladiesdf$Date[a]]][[bsimladiesdf$Distance[a]]]<-list()
  }
  if(bsimladiesdf$Name[a] %in% names(bsimladyraces[[bsimladiesdf$Seasons[a]]][[bsimladiesdf$Date[a]]][[bsimladiesdf$Distance[a]]])==FALSE){
    bsimladyraces[[bsimladiesdf$Seasons[a]]][[bsimladiesdf$Date[a]]][[bsimladiesdf$Distance[a]]][[bsimladiesdf$Name[a]]]<-as.double(bsimladiesdf$Place[a])
  }
}





savebsimmaleraces<-bsimmaleraces
savebsimladyraces <- bsimladyraces

#Initialize all skiers to have an Elo of 1300
bsimmaleelo = list()
bsimladyelo = list()
for(a in 1:length(bsimmendf$Name)){
  if(bsimmendf$Name[a] %in% names(bsimmaleelo) == FALSE){
    bsimmaleelo[[bsimmendf$Name[a]]] <- list()
    bsimmaleelo[[bsimmendf$Name[a]]][["0000"]]<-list()
    bsimmaleelo[[bsimmendf$Name[a]]][["0000"]][["00000000"]]<-list()
    bsimmaleelo[[bsimmendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(bsimladiesdf$Name)){
  if(bsimladiesdf$Name[a] %in% names(bsimladyelo) == FALSE){
    bsimladyelo[[bsimladiesdf$Name[a]]] <- list()
    bsimladyelo[[bsimladiesdf$Name[a]]][["0000"]]<-list()
    bsimladyelo[[bsimladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    bsimladyelo[[bsimladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savebsimmaleelo<-bsimmaleelo
savebsimladyelo <- bsimladyelo

#first attempt at an elo rating
bsimmaleelo<-savebsimmaleelo

albsimlelo <- c()
K=1
place_index = 1

for(z in 1:length(bsimmaleraces)){
  print(z)
  
  for(a in 1:length(bsimmaleraces[[z]])){
    
    
    for(b in 1:length(bsimmaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(bsimmaleraces[[z]][[a]][[b]])){
        lastp = bsimmaleelo[[names(bsimmaleraces[[z]][[a]][[b]][c])]][[length(bsimmaleelo[[names(bsimmaleraces[[z]][[a]][[b]][c])]])]][[length(bsimmaleelo[[names(bsimmaleraces[[z]][[a]][[b]][c])]][[length(bsimmaleelo[[names(bsimmaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(bsimmaleraces[[z]][[a]][[b]]), as.double(bsimmendf$Place[place_index:(place_index+length(names(bsimmaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(bsimmaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(bsimmaleraces[[z]][[a]][[b]][c])
        current_year <- names(bsimmaleraces[z])
        if(current_year %in% names(bsimmaleelo[[names(bsimmaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          bsimmaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(bsimmaleraces[[z]][a])
        if(current_date %in% names(bsimmaleelo[[names(bsimmaleraces[[z]][[a]][[b]][c])]][[names(bsimmaleraces[z])]])==FALSE){
          bsimmaleelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(bsimmaleraces[[z]][[a]][b])
        if(current_race %in% names(bsimmaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          bsimmaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        albsimlelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(bsimmaleelo)){
    if(length(bsimmaleelo[[d]])>1){
      plastp = bsimmaleelo[[d]][[length(bsimmaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #bsimmaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      bsimmaleelo[[d]][[current_year]][[current_date]] <- list()
      bsimmaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
bsimladyelo <- savebsimladyelo
place_index = 1
albsimlelo <- c()
for(z in 1:length(bsimladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(bsimladyraces[[z]])){
    
    
    for(b in 1:length(bsimladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(bsimladyraces[[z]][[a]][[b]])){
        lastp = bsimladyelo[[names(bsimladyraces[[z]][[a]][[b]][c])]][[length(bsimladyelo[[names(bsimladyraces[[z]][[a]][[b]][c])]])]][[length(bsimladyelo[[names(bsimladyraces[[z]][[a]][[b]][c])]][[length(bsimladyelo[[names(bsimladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(bsimladyraces[[z]][[a]][[b]]), as.double(bsimladiesdf$Place[place_index:(place_index+length(names(bsimladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(bsimladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(bsimladyraces[[z]][[a]][[b]][c])
        current_year <- names(bsimladyraces[z])
        if(current_year %in% names(bsimladyelo[[names(bsimladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          bsimladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(bsimladyraces[[z]][a])
        if(current_date %in% names(bsimladyelo[[names(bsimladyraces[[z]][[a]][[b]][c])]][[names(bsimladyraces[z])]])==FALSE){
          bsimladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(bsimladyraces[[z]][[a]][b])
        if(current_race %in% names(bsimladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          bsimladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        albsimlelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(bsimladyelo)){
    if(length(bsimladyelo[[d]])>1){
      plastp = bsimladyelo[[d]][[length(bsimladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #bsimladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      bsimladyelo[[d]][[current_year]][[current_date]] <- list()
      bsimladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}

#Now time to make a data frame

#Name, Season, Date, Distance, Elo
bsimmaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

bsimmnames = c()
blseason = c()
bsimmdate = c()
bsimmdistance = c()
bsimmelo = c()
for (a in 1:length(bsimmaleelo)){
  tick=0
  for(b in 1:length(bsimmaleelo[[a]])){
    for(c in 1:length(bsimmaleelo[[a]][[b]])){
      for(d in 1:length(bsimmaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #bsimmnames2 = append(bsimmnames2,as.character(names(bsimmaleelo[a])))
      }
    }
  }
  bsimmnames = append(bsimmnames, rep(as.character(names(bsimmaleelo[a])), tick))
}


for (a in 1:length(bsimmaleelo)){
  for(b in 1:length(bsimmaleelo[[a]])){
    for(c in 1:length(bsimmaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(bsimmaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      bsimmdate=append(bsimmdate, rep(as.character(names(bsimmaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(bsimmaleelo)){
  for(b in 1:length(bsimmaleelo[[a]])){
    for(c in 1:length(bsimmaleelo[[a]][[b]])){
      for(d in 1:length(bsimmaleelo[[a]][[b]][[c]])){
        bsimmdistance=append(bsimmdistance, as.character(names(bsimmaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(bsimmaleelo)){
  for(b in 1:length(bsimmaleelo[[a]])){
    for(c in 1:length(bsimmaleelo[[a]][[b]])){
      for(d in 1:length(bsimmaleelo[[a]][[b]][[c]])){
        bsimmelo=append(bsimmelo, as.double(bsimmaleelo[[a]][[b]][[c]][[d]]))}}}}


bsimmaleelodf <- data.frame(bsimmnames, bsimmdate, bsimmdistance, bsimmelo)



bsimladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

bsimlnames = c()
blseason = c()
bsimldate = c()
bsimldistance = c()
bsimlelo = c()
for (a in 1:length(bsimladyelo)){
  tick=0
  for(b in 1:length(bsimladyelo[[a]])){
    for(c in 1:length(bsimladyelo[[a]][[b]])){
      for(d in 1:length(bsimladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #bsimlnames2 = append(bsimlnames2,as.character(names(bsimladyelo[a])))
      }
    }
  }
  bsimlnames = append(bsimlnames, rep(as.character(names(bsimladyelo[a])), tick))
}


for (a in 1:length(bsimladyelo)){
  for(b in 1:length(bsimladyelo[[a]])){
    for(c in 1:length(bsimladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(bsimladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      bsimldate=append(bsimldate, rep(as.character(names(bsimladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(bsimladyelo)){
  for(b in 1:length(bsimladyelo[[a]])){
    for(c in 1:length(bsimladyelo[[a]][[b]])){
      for(d in 1:length(bsimladyelo[[a]][[b]][[c]])){
        bsimldistance=append(bsimldistance, as.character(names(bsimladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(bsimladyelo)){
  for(b in 1:length(bsimladyelo[[a]])){
    for(c in 1:length(bsimladyelo[[a]][[b]])){
      for(d in 1:length(bsimladyelo[[a]][[b]][[c]])){
        bsimlelo=append(bsimlelo, as.double(bsimladyelo[[a]][[b]][[c]][[d]]))}}}}


bsimladyelodf <- data.frame(bsimlnames, bsimldate, bsimldistance, bsimlelo)

savebsimmnames <- bsimmnames
savebsimmdistance <- bsimmdistance
savebsimmdate <- bsimmdate
savebsimmelo <- bsimmelo
savebsimlnames <- bsimlnames
savebsimldistance <- bsimldistance
savebsimldate <- bsimldate
savebsimlelo <- bsimlelo


bsimmalehighest <- bsimmaleelodf[order(-bsimmaleelodf$bsimmelo), ]
bsimmalehighest_ind <- bsimmalehighest[match(unique(bsimmalehighest$bsimmnames), bsimmalehighest$bsimmnames), ]
row.names(bsimmalehighest_ind) <- 1:length(bsimmalehighest_ind[,1])
bsimmalehighest_ind[1:25,]


bsimladyhighest <- bsimladyelodf[order(-bsimladyelodf$bsimlelo), ]
bsimladyhighest_ind <-bsimladyhighest[match(unique(bsimladyhighest$bsimlnames), bsimladyhighest$bsimlnames), ]
row.names(bsimladyhighest_ind) <- 1:length(bsimladyhighest_ind[,1])
bsimladyhighest_ind[1:25, ]
  
bsimmlast_race <- bsimmaleelodf[bsimmaleelodf$bsimmdate=="20200500",]
bsimmlast_race[order(-bsimmlast_race$bsimmelo),]

library(ggplot2)

jhk = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(bsimmelo), colour=bsimmnames)) +geom_point()


bd = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = bsimmaleelodf[(bsimmaleelodf$bsimmnames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(bsimmelo), colour=bsimmnames)) +geom_point()

top <- rbind(bd, jhk, vs,bolsh,pnj,ta)
ggplot(top, aes(race, as.double(bsimmelo), colour=bsimmnames)) +geom_point()
bsimmaleelodf[which(bsimmaleelodf$bsimmnames=="Kevin Brochman"),]

jd = bsimladyelodf[(bsimladyelodf$bsimlnames)=="Jessica Diggins", ]
kr = bsimladyelodf[(bsimladyelodf$bsimlnames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(bsimlelo), colour=bsimlnames)) +geom_point()

tj = bsimladyelodf[(bsimladyelodf$bsimlnames)=="Therese Johaug", ]
mb = bsimladyelodf[(bsimladyelodf$bsimlnames)=="Marit Bjørgen", ]
io = bsimladyelodf[(bsimladyelodf$bsimlnames)=="Ingvild Flugstad Østberg", ]
hw = bsimladyelodf[(bsimladyelodf$bsimlnames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(bsimlelo), colour=bsimlnames)) +geom_point()


bsimladyelodf[(bsimladyelodf$bsimlnames)=="Rosie Brennan", ]


bsimmlast_race <- bsimmaleelodf[bsimmaleelodf$bsimmdate=="20201129",]
bsimmlast_race <- bsimmlast_race[order(-bsimmlast_race$bsimmelo),]
row.names(bsimmlast_race) <- 1:length(bsimmlast_race[,1])
bsimmlast_race[1:25,]

bsimllast_race <- bsimladyelodf[bsimladyelodf$bsimldate=="20200500", ]
bsimllast_race <- bsimllast_race[order(-bsimllast_race$bsimlelo),]
row.names(bsimllast_race) <- 1:length(bsimllast_race[,1])
bsimllast_race[1:25,]


