library("tidyverse")
library(simstudy)
library(psych)
library(SuppDists)
library(truncdist)
library(truncnorm)
library(EnvStats)
library(e1071)
subsimmen <- read_excel("~/ski/elo/excel365/all.xlsx", 
                     sheet = "Men", col_names = FALSE, na = "NA")
subsimladies <- read_excel("~/ski/elo/excel365/all.xlsx", 
                        sheet = "Ladies", col_names = FALSE, na = "NA")
insertRow <- function(existingDF, newrow, r) {
  existingDF[seq(r+1,nrow(existingDF)+1),] <- existingDF[seq(r,nrow(existingDF)),]
  existingDF[r,] <- newrow
  existingDF
}
subsimmendf <- data.frame(subsimmen)
names(subsimmendf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")
subsimladiesdf <- data.frame(subsimladies)
names(subsimladiesdf) <- c("Date", "City", "Country", "Event", "Sex", "Distance", "Discipline", "Place", "Name", "Nationality")

subsimmendf[2073,9] <- "Oddmund Jensen2"
subsimmendf[which(subsimmendf$Distance=="Stage"), 1] <- as.character(as.double(subsimmendf[which(subsimmendf$Distance=="Stage"), 1])+1)
subsimladiesdf[which(subsimladiesdf$Distance=="Stage"), 1] <- as.character(as.double(subsimladiesdf[which(subsimladiesdf$Distance=="Stage"), 1])+1)



subsimmendf$Seasons = NA
subsimladiesdf$Seasons = NA
for(a in 1:length(subsimmendf$Date)){
  if((as.double(substr(subsimmendf$Date[a], 5, 8)) > 1000) && as.double(substr(subsimmendf$Date[a],5,8)!=9999)){
    subsimmendf$Seasons[a] = as.character(as.double(substr(subsimmendf$Date[a], 1, 4))+1)
  }
  else{
    subsimmendf$Seasons[a] = as.character(as.double(substr(subsimmendf$Date[a], 1, 4)))
  }
}
for(a in 1:length(subsimladiesdf$Date)){
  if((as.double(substr(subsimladiesdf$Date[a], 5, 8)) > 1000) && as.double(substr(subsimladiesdf$Date[a],5,8)!=9999)){
    subsimladiesdf$Seasons[a] = as.character(as.double(substr(subsimladiesdf$Date[a], 1, 4))+1)
  }
  else{
    subsimladiesdf$Seasons[a] = as.character(as.double(substr(subsimladiesdf$Date[a], 1, 4)))
  }
}

subsimladiesdf[24331, 9] <- "Tatjana Kuznetsova2"

for(a in 1982:2020){
  print(a)
  cur_seasondf <- subsimmendf[subsimmendf$Seasons==as.character(a), ]
  #
  num_racers <- as.numeric(as.matrix(table(cur_seasondf$Date)))
  num_races <- length(unique(cur_seasondf$Date))
  num_subsimraces <- max(c(0, num_races-10))
  num_simraces <- max(c(10-num_races, 0))
  if(num_subsimraces>0){
    for(b in 1:num_subsimraces){
      subsimrand <- sample(unique(cur_seasondf$Date), 1)
      subsimmendf<- subsimmendf[!(subsimmendf$Date==subsimrand), ]
      cur_seasondf <- cur_seasondf[!(cur_seasondf$Date==subsimrand), ]
      
    }
  }
}
  
#   # print(table(name_pot))
#   subsim_results <- c()
#   for(z in 1: num_subsimraces){
#     name_pot <- cur_seasondf$Name
#     # print(table(name_pot))
#     name_pot_del <- names(table(name_pot)[which(as.numeric(as.matrix(table(name_pot)))<2)])
#     #print(name_pot_del)
#     name_pot = name_pot[! name_pot %in% name_pot_del]
#     #print(length(unique(name_pot)))
#     
#     if(max(num_racers)+1 > length(unique(name_pot))){
#       subsimnum_racers <- as.integer(rtruncnorm(1, min(num_racers), length(unique(name_pot)), mean(num_racers), sd(num_racers)))
#     }
#     else{
#       subsimnum_racers <- as.integer(rtruncnorm(1, min(num_racers), max(num_racers)+1, mean(num_racers), sd(num_racers)))
#     }
#     #print(subsimnum_racers)
#     
#     
#     
#     
#     name_sample <- c()
#     for(b in 1:subsimnum_racers){
#       subsim_racer <- sample(name_pot, 1)
#       
#       #print(subsim_racer)
#       name_sample <- append(name_sample, subsim_racer)
#       name_pot <- name_pot[! name_pot %in% subsim_racer]
#       #setdiff[name_pot, subsim_racer]
#     }
#     #print(name_sample)
#     #print(name_sample)
#     
#     (cur_seasondf[cur_seasondf$Name=="Sung-Hoon Cho", ])
#     #cur_seasondf[which(cur_seasondf$Name %in% name_sample), ]
#     name_sample <- (name_sample[order(name_sample)])
#     # print(name_sample)
#     pnorm(1, mean=mean(as.double(cur_seasondf[name_sample,8])), 
#           sd=sd(as.double(cur_seasondf[name_sample,8])))
#     
#     results = c()
#     subsim_nation <- c()
#     for(c in 1:(subsimnum_racers)){
#       pos_prob_tot=0
#       raw_pnorms = c()
#       
#       for(d in 1:length(name_sample)){
#         
#         raw_pnorm <- pnorm(c, mean=mean(as.double(cur_seasondf[cur_seasondf$Name==name_sample[d],8])), 
#                            sd=sd(as.double(cur_seasondf[cur_seasondf$Name==name_sample[d],8])))
#         # print(name_sample[d])
#         # print(raw_pnorm)
#         # print(raw_pnorm)
#         raw_pnorms <- append(raw_pnorms, raw_pnorm)
#         pos_prob_tot <- pos_prob_tot +pnorm(c, mean=mean(as.double(cur_seasondf[cur_seasondf$Name==name_sample[d],8])), 
#                                             sd=sd(as.double(cur_seasondf[cur_seasondf$Name==name_sample[d],8])))
#         #print(name_sample[c])
#         #print(pnorm(1, mean=mean(as.double(cur_seasondf[cur_seasondf$Name==name_sample[c],8])), 
#         #sd=sd(as.double(cur_seasondf[cur_seasondf$Name==name_sample[c],8]))))
#         
#       }
#       adj_pnorms <- raw_pnorms/pos_prob_tot
#       if(is.na(adj_pnorms[length(adj_pnorms)])){
#         pos <- name_sample[length(adj_pnorms)]
#       }
#       else{
#         pos <- sample(name_sample, 1, prob=adj_pnorms)
#       }
#       subsim_nation = (append(subsim_nation, cur_seasondf[which(cur_seasondf$Name==pos)[1], 10]))
#       # print(cur_seasondf[which(cur_seasondf$Name==pos)[1], 10])
#       #print(name_sample)
#       #print(pos)
#       name_sample <- name_sample[! name_sample %in% pos]
#       results = append(results, pos)
#       #print(results)
#     }
#     results_names <- results
#     results_pos <- 1:length(results_names)
#     #print(results_names)
#     results_city <- rep("Sim City", length(results_names))
#     results_country <- rep("Simland", length(results_names))
#     results_event <- rep("Sim", length(results_names))
#     results_sex <- rep("M", length(results_names))
#     results_distance <- rep("Sim", length(results_names))
#     results_discipline <- rep("Sim", length(results_names))
#     results_date <- rep(as.character(as.double(cur_seasondf[length(cur_seasondf$Date), 1])+1), length(results_names))
#     results_season <- rep(a, length(results_names))
#     
#     
#     subsim_results<-data.frame(as.character(results_date), as.character(results_city), as.character(results_country), as.character(results_event), as.character(results_sex), 
#                             as.character(results_distance), as.character(results_discipline), as.character(results_pos), as.character(results_names),
#                             as.character(subsim_nation), as.character(results_season))
#     
#     names(subsim_results) <- names(cur_seasondf)
#     #print(subsim_results)
#     
#     cur_seasondf <- rbind(cur_seasondf, subsim_results)
#     #subsim_results = append(subsim_results, results)
#   }
#   #Box cox if you need it later
#   # box.cox <- boxcox(as.double(cur_seasondf[cur_seasondf$Name=="Johannes Høsflot Klæbo",8]), optimize=TRUE)$lambda
#   # print(box.cox)
#   # temp_data <- as.double(cur_seasondf[cur_seasondf$Name=="Alexander Bolshunov",8])
#   # temp_data <- temp_data^box.cox
#   
#   #print(pnorm(1^box.cox, mean=mean(temp_data), sd=sd(temp_data)))
#   #print(shapiro.test(temp_places))
#   # name_list <- unique(cur_seasondf$Name)
#   # name_list.probs <- table(name_pot)/length(name_pot)
#   # name_sample <- sample(x=name_list, size=subsimnum_racers, replace=FALSE, prob=as.numeric(as.matrix(name_list.probs)))
#   # print(name_sample[order(name_sample)])
#   subsimmendf <- subsimmendf[-which(subsimmendf$Seasons==as.character(a)), ]
#   subsimmendf <- rbind(subsimmendf, cur_seasondf)
# }

for(a in 1982:2020){
  print(a)
  lcur_seasondf <- subsimladiesdf[subsimladiesdf$Seasons==as.character(a), ]
  #
  lnum_racers <- as.numeric(as.matrix(table(lcur_seasondf$Date)))
  lnum_races <- length(unique(lcur_seasondf$Date))
  lnum_subsimraces <- max(c(0, lnum_races-10))
  lnum_simraces <- max(c(10-lnum_races, 0))
  if(lnum_subsimraces>0){
    for(b in 1:lnum_subsimraces){
      subsimrand <- sample(unique(lcur_seasondf$Date), 1)
      subsimladiesdf<- subsimladiesdf[!(subsimladiesdf$Date==subsimrand), ]
      lcur_seasondf <- lcur_seasondf[!(lcur_seasondf$Date==subsimrand), ]
      
    }
  }
}
  
  
#   # print(table(name_lpot))
#   subsim_lresults <- c()
#   for(z in 1: lnum_subsimraces){
#     name_lpot <- lcur_seasondf$Name
#     # print(table(name_lpot))
#     name_lpot_del <- names(table(name_lpot)[which(as.numeric(as.matrix(table(name_lpot)))<2)])
#     #print(name_lpot_del)
#     name_lpot = name_lpot[! name_lpot %in% name_lpot_del]
#     #print(length(unique(name_lpot)))
#     
#     if(max(lnum_racers)+1 > length(unique(name_lpot))){
#       subsimlnum_racers <- as.integer(rtruncnorm(1, min(lnum_racers), length(unique(name_lpot)), mean(lnum_racers), sd(lnum_racers)))
#     }
#     else{
#       subsimlnum_racers <- as.integer(rtruncnorm(1, min(lnum_racers), max(lnum_racers)+1, mean(lnum_racers), sd(lnum_racers)))
#     }
#     #print(subsimlnum_racers)
#     
#     
#     
#     
#     lname_sample <- c()
#     for(b in 1:subsimlnum_racers){
#       lsubsim_racer <- sample(name_lpot, 1)
#       
#       #print(lsubsim_racer)
#       lname_sample <- append(lname_sample, lsubsim_racer)
#       name_lpot <- name_lpot[! name_lpot %in% lsubsim_racer]
#       #setdiff[name_lpot, lsubsim_racer]
#     }
#     #print(lname_sample)
#     #print(lname_sample)
#     
#     (lcur_seasondf[lcur_seasondf$Name=="Sung-Hoon Cho", ])
#     #lcur_seasondf[which(lcur_seasondf$Name %in% lname_sample), ]
#     lname_sample <- (lname_sample[order(lname_sample)])
#     # print(lname_sample)
#     pnorm(1, mean=mean(as.double(lcur_seasondf[lname_sample,8])), 
#           sd=sd(as.double(lcur_seasondf[lname_sample,8])))
#     
#     lresults = c()
#     subsim_lnation <- c()
#     for(c in 1:(subsimlnum_racers)){
#       lpos_prob_tot=0
#       lraw_pnormss = c()
#       
#       for(d in 1:length(lname_sample)){
#         
#         lraw_pnorms <- pnorm(c, mean=mean(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[d],8])), 
#                              sd=sd(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[d],8])))
#         # print(lname_sample[d])
#         # print(lraw_pnorms)
#         # print(lraw_pnorms)
#         lraw_pnormss <- append(lraw_pnormss, lraw_pnorms)
#         lpos_prob_tot <- lpos_prob_tot +pnorm(c, mean=mean(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[d],8])), 
#                                               sd=sd(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[d],8])))
#         #print(lname_sample[c])
#         #print(pnorm(1, mean=mean(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[c],8])), 
#         #sd=sd(as.double(lcur_seasondf[lcur_seasondf$Name==lname_sample[c],8]))))
#         
#       }
#       lasj_pnorms <- lraw_pnormss/lpos_prob_tot
#       if(is.na(lasj_pnorms[length(lasj_pnorms)])){
#         lpos <- lname_sample[length(lasj_pnorms)]
#       }
#       else{
#         lpos <- sample(lname_sample, 1, prob=lasj_pnorms)
#       }
#       subsim_lnation = (append(subsim_lnation, lcur_seasondf[which(lcur_seasondf$Name==lpos)[1], 10]))
#       # print(lcur_seasondf[which(lcur_seasondf$Name==lpos)[1], 10])
#       #print(lname_sample)
#       #print(lpos)
#       lname_sample <- lname_sample[! lname_sample %in% lpos]
#       lresults = append(lresults, lpos)
#       #print(lresults)
#     }
#     lresults_names <- lresults
#     lresults_lpos <- 1:length(lresults_names)
#     #print(lresults_names)
#     lresults_city <- rep("Sim City", length(lresults_names))
#     lresults_country <- rep("Simland", length(lresults_names))
#     lresults_event <- rep("Sim", length(lresults_names))
#     lresults_sex <- rep("M", length(lresults_names))
#     lresults_distance <- rep("Sim", length(lresults_names))
#     lresults_discipline <- rep("Sim", length(lresults_names))
#     lresults_date <- rep(as.character(as.double(lcur_seasondf[length(lcur_seasondf$Date), 1])+1), length(lresults_names))
#     lresults_season <- rep(a, length(lresults_names))
#     
#     
#     subsim_lresults<-data.frame(as.character(lresults_date), as.character(lresults_city), as.character(lresults_country), as.character(lresults_event), as.character(lresults_sex), 
#                              as.character(lresults_distance), as.character(lresults_discipline), as.character(lresults_lpos), as.character(lresults_names),
#                              as.character(subsim_lnation), as.character(lresults_season))
#     
#     names(subsim_lresults) <- names(lcur_seasondf)
#     #print(subsim_lresults)
#     
#     lcur_seasondf <- rbind(lcur_seasondf, subsim_lresults)
#     #subsim_lresults = append(subsim_lresults, lresults)
#   }
#   #Box cox if you need it later
#   # box.cox <- boxcox(as.double(lcur_seasondf[lcur_seasondf$Name=="Johannes Høsflot Klæbo",8]), optimize=TRUE)$lambda
#   # print(box.cox)
#   # temp_data <- as.double(lcur_seasondf[lcur_seasondf$Name=="Alexander Bolshunov",8])
#   # temp_data <- temp_data^box.cox
#   
#   #print(pnorm(1^box.cox, mean=mean(temp_data), sd=sd(temp_data)))
#   #print(shapiro.test(temp_places))
#   # name_list <- unique(lcur_seasondf$Name)
#   # name_list.probs <- table(name_lpot)/length(name_lpot)
#   # lname_sample <- sample(x=name_list, size=subsimlnum_racers, replace=FALSE, prob=as.numeric(as.matrix(name_list.probs)))
#   # print(lname_sample[order(lname_sample)])
#   subsimladiesdf <- subsimladiesdf[-which(subsimladiesdf$Seasons==as.character(a)), ]
#   subsimladiesdf <- rbind(subsimladiesdf, lcur_seasondf)
# }
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
subsimmaleraces = list()
subsimladyraces = list()

for(a in 1:length(subsimmendf$Seasons)){
  if(subsimmendf$Seasons[a] %in% names(subsimmaleraces)==FALSE){
    subsimmaleraces[[subsimmendf$Seasons[a]]] <- list()
  }
  
  if(subsimmendf$Date[a] %in% names(subsimmaleraces[[subsimmendf$Seasons[a]]])==FALSE){
    subsimmaleraces[[subsimmendf$Seasons[a]]][[subsimmendf$Date[a]]] <- list()
  }
  if(subsimmendf$Distance[a] %in% names(subsimmaleraces[[subsimmendf$Seasons[a]]][[subsimmendf$Date[a]]])==FALSE){
    subsimmaleraces[[subsimmendf$Seasons[a]]][[subsimmendf$Date[a]]][[subsimmendf$Distance[a]]]<-list()
  }
  if(subsimmendf$Name[a] %in% names(subsimmaleraces[[subsimmendf$Seasons[a]]][[subsimmendf$Date[a]]][[subsimmendf$Distance[a]]])==FALSE){
    subsimmaleraces[[subsimmendf$Seasons[a]]][[subsimmendf$Date[a]]][[subsimmendf$Distance[a]]][[subsimmendf$Name[a]]]<-as.double(subsimmendf$Place[a])
  }
}

for(a in 1:length(subsimladiesdf$Seasons)){
  if(subsimladiesdf$Seasons[a] %in% names(subsimladyraces)==FALSE){
    subsimladyraces[[subsimladiesdf$Seasons[a]]] <- list()
  }
  
  if(subsimladiesdf$Date[a] %in% names(subsimladyraces[[subsimladiesdf$Seasons[a]]])==FALSE){
    subsimladyraces[[subsimladiesdf$Seasons[a]]][[subsimladiesdf$Date[a]]] <- list()
  }
  if(subsimladiesdf$Distance[a] %in% names(subsimladyraces[[subsimladiesdf$Seasons[a]]][[subsimladiesdf$Date[a]]])==FALSE){
    subsimladyraces[[subsimladiesdf$Seasons[a]]][[subsimladiesdf$Date[a]]][[subsimladiesdf$Distance[a]]]<-list()
  }
  if(subsimladiesdf$Name[a] %in% names(subsimladyraces[[subsimladiesdf$Seasons[a]]][[subsimladiesdf$Date[a]]][[subsimladiesdf$Distance[a]]])==FALSE){
    subsimladyraces[[subsimladiesdf$Seasons[a]]][[subsimladiesdf$Date[a]]][[subsimladiesdf$Distance[a]]][[subsimladiesdf$Name[a]]]<-as.double(subsimladiesdf$Place[a])
  }
}





savesubsimmaleraces<-subsimmaleraces
savesubsimladyraces <- subsimladyraces

#Initialize all skiers to have an Elo of 1300
subsimmaleelo = list()
subsimladyelo = list()
for(a in 1:length(subsimmendf$Name)){
  if(subsimmendf$Name[a] %in% names(subsimmaleelo) == FALSE){
    subsimmaleelo[[subsimmendf$Name[a]]] <- list()
    subsimmaleelo[[subsimmendf$Name[a]]][["0000"]]<-list()
    subsimmaleelo[[subsimmendf$Name[a]]][["0000"]][["00000000"]]<-list()
    subsimmaleelo[[subsimmendf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
for(a in 1:length(subsimladiesdf$Name)){
  if(subsimladiesdf$Name[a] %in% names(subsimladyelo) == FALSE){
    subsimladyelo[[subsimladiesdf$Name[a]]] <- list()
    subsimladyelo[[subsimladiesdf$Name[a]]][["0000"]]<-list()
    subsimladyelo[[subsimladiesdf$Name[a]]][["0000"]][["00000000"]]<-list()
    subsimladyelo[[subsimladiesdf$Name[a]]][["0000"]][["00000000"]][["0"]] <- 1300
  }
}
savesubsimmaleelo<-subsimmaleelo
savesubsimladyelo <- subsimladyelo

#first attempt at an elo rating
subsimmaleelo<-savesubsimmaleelo

alsubsimlelo <- c()
K=1
place_index = 1

for(z in 1:length(subsimmaleraces)){
  print(z)
  
  for(a in 1:length(subsimmaleraces[[z]])){
    
    
    for(b in 1:length(subsimmaleraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(subsimmaleraces[[z]][[a]][[b]])){
        lastp = subsimmaleelo[[names(subsimmaleraces[[z]][[a]][[b]][c])]][[length(subsimmaleelo[[names(subsimmaleraces[[z]][[a]][[b]][c])]])]][[length(subsimmaleelo[[names(subsimmaleraces[[z]][[a]][[b]][c])]][[length(subsimmaleelo[[names(subsimmaleraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      race_df <- data.frame(names(subsimmaleraces[[z]][[a]][[b]]), as.double(subsimmendf$Place[place_index:(place_index+length(names(subsimmaleraces[[z]][[a]][[b]]))-1)]), pelo)
      place_index = place_index+ length(names(subsimmaleraces[[z]][[a]][[b]]))
      names(race_df) <-c("Name", "Place", "pelo")
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(subsimmaleraces[[z]][[a]][[b]][c])
        current_year <- names(subsimmaleraces[z])
        if(current_year %in% names(subsimmaleelo[[names(subsimmaleraces[[z]][[a]][[b]][c])]]) == FALSE){
          subsimmaleelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(subsimmaleraces[[z]][a])
        if(current_date %in% names(subsimmaleelo[[names(subsimmaleraces[[z]][[a]][[b]][c])]][[names(subsimmaleraces[z])]])==FALSE){
          subsimmaleelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(subsimmaleraces[[z]][[a]][b])
        if(current_race %in% names(subsimmaleelo[[current_man]][[current_year]][[current_date]])==FALSE){
          subsimmaleelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        alsubsimlelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(subsimmaleelo)){
    if(length(subsimmaleelo[[d]])>1){
      plastp = subsimmaleelo[[d]][[length(subsimmaleelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #subsimmaleelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      subsimmaleelo[[d]][[current_year]][[current_date]] <- list()
      subsimmaleelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}
subsimladyelo <- savesubsimladyelo
place_index = 1
alsubsimlelo <- c()
for(z in 1:length(subsimladyraces)){
  
  #for(z in 1:43){
  #print(z)
  
  for(a in 1:length(subsimladyraces[[z]])){
    
    
    for(b in 1:length(subsimladyraces[[z]][[a]])){
      pelo = c()
      elo = c()
      
      for(c in 1:length(subsimladyraces[[z]][[a]][[b]])){
        lastp = subsimladyelo[[names(subsimladyraces[[z]][[a]][[b]][c])]][[length(subsimladyelo[[names(subsimladyraces[[z]][[a]][[b]][c])]])]][[length(subsimladyelo[[names(subsimladyraces[[z]][[a]][[b]][c])]][[length(subsimladyelo[[names(subsimladyraces[[z]][[a]][[b]][c])]])]])]]
        lastpp = lastp[[length(lastp)]]
        pelo = append(pelo, lastpp)                   
      }
      
      race_df <- data.frame(names(subsimladyraces[[z]][[a]][[b]]), as.double(subsimladiesdf$Place[place_index:(place_index+length(names(subsimladyraces[[z]][[a]][[b]]))-1)]), pelo)
      
      
      place_index = place_index+ length(names(subsimladyraces[[z]][[a]][[b]]))
      
      names(race_df) <-c("Name", "Place", "pelo")
      
      #  last = max(race_df$Place)
      #Now we calculate the Elo for each racer
      for (c in 1:length(race_df$pelo)){
        #Check to see if there is a list for season for the racer
        current_man <- names(subsimladyraces[[z]][[a]][[b]][c])
        current_year <- names(subsimladyraces[z])
        if(current_year %in% names(subsimladyelo[[names(subsimladyraces[[z]][[a]][[b]][c])]]) == FALSE){
          subsimladyelo[[current_man]][[current_year]] <- list()
        }
        #Check to see if there is a list for date for the racer
        current_date <- names(subsimladyraces[[z]][a])
        if(current_date %in% names(subsimladyelo[[names(subsimladyraces[[z]][[a]][[b]][c])]][[names(subsimladyraces[z])]])==FALSE){
          subsimladyelo[[current_man]][[current_year]][[current_date]]<-list()
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
        
        current_race <- names(subsimladyraces[[z]][[a]][b])
        if(current_race %in% names(subsimladyelo[[current_man]][[current_year]][[current_date]])==FALSE){
          subsimladyelo[[current_man]][[current_year]][[current_date]][[current_race]] <- r11
        }
        elo = append(elo, r11)
        alsubsimlelo=append(elo, r11)
      }
    }
  }
  for(d in 1:length(subsimladyelo)){
    if(length(subsimladyelo[[d]])>1){
      plastp = subsimladyelo[[d]][[length(subsimladyelo[[d]])]]
      plastp2 = plastp[[length(plastp)]][[length(length(plastp))]]
      #subsimladyelo[[d]]<-list()
      current_date = paste(current_year, "0500", sep="")
      subsimladyelo[[d]][[current_year]][[current_date]] <- list()
      subsimladyelo[[d]][[current_year]][[current_date]][["0"]] <- plastp2*.85+1300*.15
    }
  }
}

#Now time to make a data frame

#Name, Season, Date, Distance, Elo
subsimmaleelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

subsimmnames = c()
blseason = c()
subsimmdate = c()
subsimmdistance = c()
subsimmelo = c()
for (a in 1:length(subsimmaleelo)){
  tick=0
  for(b in 1:length(subsimmaleelo[[a]])){
    for(c in 1:length(subsimmaleelo[[a]][[b]])){
      for(d in 1:length(subsimmaleelo[[a]][[b]][[c]])){
        tick = tick+1
        #subsimmnames2 = append(subsimmnames2,as.character(names(subsimmaleelo[a])))
      }
    }
  }
  subsimmnames = append(subsimmnames, rep(as.character(names(subsimmaleelo[a])), tick))
}


for (a in 1:length(subsimmaleelo)){
  for(b in 1:length(subsimmaleelo[[a]])){
    for(c in 1:length(subsimmaleelo[[a]][[b]])){
      tick=0
      for(d in 1:length(subsimmaleelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      subsimmdate=append(subsimmdate, rep(as.character(names(subsimmaleelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(subsimmaleelo)){
  for(b in 1:length(subsimmaleelo[[a]])){
    for(c in 1:length(subsimmaleelo[[a]][[b]])){
      for(d in 1:length(subsimmaleelo[[a]][[b]][[c]])){
        subsimmdistance=append(subsimmdistance, as.character(names(subsimmaleelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(subsimmaleelo)){
  for(b in 1:length(subsimmaleelo[[a]])){
    for(c in 1:length(subsimmaleelo[[a]][[b]])){
      for(d in 1:length(subsimmaleelo[[a]][[b]][[c]])){
        subsimmelo=append(subsimmelo, as.double(subsimmaleelo[[a]][[b]][[c]][[d]]))}}}}


subsimmaleelodf <- data.frame(subsimmnames, subsimmdate, subsimmdistance, subsimmelo)



subsimladyelodf <- data.frame()#"Name"=c(), "Season"=c())#, "Date"=character(), "Distance"=character(), "Elo"=double()) 

subsimlnames = c()
blseason = c()
subsimldate = c()
subsimldistance = c()
subsimlelo = c()
for (a in 1:length(subsimladyelo)){
  tick=0
  for(b in 1:length(subsimladyelo[[a]])){
    for(c in 1:length(subsimladyelo[[a]][[b]])){
      for(d in 1:length(subsimladyelo[[a]][[b]][[c]])){
        tick = tick+1
        #subsimlnames2 = append(subsimlnames2,as.character(names(subsimladyelo[a])))
      }
    }
  }
  subsimlnames = append(subsimlnames, rep(as.character(names(subsimladyelo[a])), tick))
}


for (a in 1:length(subsimladyelo)){
  for(b in 1:length(subsimladyelo[[a]])){
    for(c in 1:length(subsimladyelo[[a]][[b]])){
      tick=0
      for(d in 1:length(subsimladyelo[[a]][[b]][[c]])){
        tick=tick+1
      }
      subsimldate=append(subsimldate, rep(as.character(names(subsimladyelo[[a]][[b]][c])), tick))
    }
  }
}



for (a in 1:length(subsimladyelo)){
  for(b in 1:length(subsimladyelo[[a]])){
    for(c in 1:length(subsimladyelo[[a]][[b]])){
      for(d in 1:length(subsimladyelo[[a]][[b]][[c]])){
        subsimldistance=append(subsimldistance, as.character(names(subsimladyelo[[a]][[b]][[c]][d])))}}}}

for (a in 1:length(subsimladyelo)){
  for(b in 1:length(subsimladyelo[[a]])){
    for(c in 1:length(subsimladyelo[[a]][[b]])){
      for(d in 1:length(subsimladyelo[[a]][[b]][[c]])){
        subsimlelo=append(subsimlelo, as.double(subsimladyelo[[a]][[b]][[c]][[d]]))}}}}


subsimladyelodf <- data.frame(subsimlnames, subsimldate, subsimldistance, subsimlelo)

savesubsimmnames <- subsimmnames
savesubsimmdistance <- subsimmdistance
savesubsimmdate <- subsimmdate
savesubsimmelo <- subsimmelo
savesubsimlnames <- subsimlnames
savesubsimldistance <- subsimldistance
savesubsimldate <- subsimldate
savesubsimlelo <- subsimlelo


subsimmalehighest <- subsimmaleelodf[order(-subsimmaleelodf$subsimmelo), ]
subsimmalehighest_ind <- subsimmalehighest[match(unique(subsimmalehighest$subsimmnames), subsimmalehighest$subsimmnames), ]
row.names(subsimmalehighest_ind) <- 1:length(subsimmalehighest_ind[,1])
subsimmalehighest_ind[1:25,]


subsimladyhighest <- subsimladyelodf[order(-subsimladyelodf$subsimlelo), ]
subsimladyhighest_ind <-subsimladyhighest[match(unique(subsimladyhighest$subsimlnames), subsimladyhighest$subsimlnames), ]
row.names(subsimladyhighest_ind) <- 1:length(subsimladyhighest_ind[,1])
subsimladyhighest_ind[1:25, ]

subsimmlast_race <- subsimmaleelodf[subsimmaleelodf$subsimmdate=="20200500",]
subsimmlast_race[order(-mlast_race$subsimmelo),]

library(ggplot2)

jhk = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Johannes Høsflot Klæbo"),]
jhk$race = 1:length(jhk[,1])
bolsh = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Alexander Bolshunov"),]
bolsh$race = 1:length(bolsh[,1])
pnj = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Petter Northug"),]
pnj$race = 1:length(pnj[,1])
dc = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Dario Cologna"),]
dc$race = 1:length(dc[,1])
mjs = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Martin Johnsrud Sundby"),]
mjs$race = 1:length(mjs[,1])
su = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Sergey Ustiugov"),]
su$race = 1:length(su[,1])
jbp <- rbind(jhk, bolsh, pnj,dc,mjs, su)
ggplot(jbp, aes(race, as.double(subsimmelo), colour=subsimmnames)) +geom_point()


bd = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Bjørn Dæhlie"),]
bd$race = 1:length(bd[,1])
gs = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Gunde Svan"),]
gs$race = 1:length(gs[,1])
vs = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Vladimir Smirnov"),]
vs$race = 1:length(vs[,1])
tm = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Torgny Mogren"),]
tm$race = 1:length(tm[,1])
vu = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Vegard Ulvang"),]
vu$race = 1:length(vu[,1])
mm = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Mika Myllylä"),]
mm$race = 1:length(mm[,1])
ta = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Thomas Alsgaard"),]
ta$race = 1:length(ta[,1])
tw = subsimmaleelodf[(subsimmaleelodf$subsimmnames=="Thomas Wassberg"),]
tw$race = 1:length(tw[,1])

old <- rbind(bd, gs, vs,tm,vu, mm,ta,tw)
ggplot(old, aes(race, as.double(subsimmelo), colour=subsimmnames)) +geom_point()

top <- rbind(bd, jhk, vs,bolsh,pnj,ta)
ggplot(top, aes(race, as.double(subsimmelo), colour=subsimmnames)) +geom_point()
subsimmaleelodf[which(subsimmaleelodf$subsimmnames=="Kevin Brochman"),]

jd = subsimladyelodf[(subsimladyelodf$subsimlnames)=="Jessica Diggins", ]
kr = subsimladyelodf[(subsimladyelodf$subsimlnames)=="Kikkan Randall", ]
jd$race = 1:length(jd[,1])
kr$race = 1:length(kr[,1])
jk <- rbind(jd, kr)
ggplot(jk, aes(race, as.double(subsimlelo), colour=subsimlnames)) +geom_point()

tj = subsimladyelodf[(subsimladyelodf$subsimlnames)=="Therese Johaug", ]
mb = subsimladyelodf[(subsimladyelodf$subsimlnames)=="Marit Bjørgen", ]
io = subsimladyelodf[(subsimladyelodf$subsimlnames)=="Ingvild Flugstad Østberg", ]
hw = subsimladyelodf[(subsimladyelodf$subsimlnames)=="Heidi Weng", ]
tj$race = 1:length(tj[,1])
mb$race = 1:length(mb[,1])
io$race = 1:length(io[,1])
hw$race = 1:length(hw[,1])
tm <- rbind(tj, mb, io, hw)
ggplot(tm, aes(race, as.double(subsimlelo), colour=subsimlnames)) +geom_point()


subsimladyelodf[(subsimladyelodf$subsimlnames)=="Rosie Brennan", ]


subsimmlast_race <- subsimmaleelodf[subsimmaleelodf$subsimmdate=="20200500",]
subsimmlast_race <- subsimmlast_race[order(-subsimmlast_race$subsimmelo),]
row.names(subsimmlast_race) <- 1:length(subsimmlast_race[,1])
subsimmlast_race[1:25,]

subsimllast_race <- subsimladyelodf[subsimladyelodf$subsimldate=="20200500", ]
subsimllast_race <- subsimllast_race[order(-subsimllast_race$subsimlelo),]
row.names(subsimllast_race) <- 1:length(subsimllast_race[,1])
subsimllast_race[1:25,]


