maleelo_update = maleelo_write
ladyelo_update = ladyelo_write

maleelo_update = maleelo_update[which(maleelo_update$maledate=="20200500"), ]
ladyelo_update  = ladyelo_update[which(ladyelo_update$ladydate=="20200500"),]
library(readxl)
library(ggplot2)
update_menxl <- read_excel("~/ski/elo/excel365/all2021.xlsx", 
                  sheet = "Men", col_names = FALSE, na = "NA")
update_ladiesxl <- read_excel("~/ski/elo/excel365/all2021.xlsx", 
                     sheet = "Ladies", col_names = FALSE, na = "NA")
update_menxl = data.frame(update_menxl)
update_ladiesxl = data.frame(update_ladiesxl)
colnames(update_menxl) = c("date", "city", "country", "type", "sex", "distance", "discipline", 
                            "place", "name", "nationality")

colnames(update_ladiesxl) = c("date", "city", "country", "type", "sex", "distance", "discipline", 
                           "place", "name", "nationality")
##Update the races for the gender and calculate their elo score
race_num = c(1)
race_n = 1
for(a in 2:length(update_menxl[,1])){
  if(as.double(as.character(update_menxl$place[a]))>=as.double(as.character(update_menxl$place[a-1]))){
    race_num = append(race_num, race_n)
  }
  else{
    #print(update_menxl[a,])
    race_n = race_n+1
    race_num = append(race_num, race_n)
  }
}

update_menxl$race = race_num
maleelo_update$malenames = as.character(maleelo_update$malenames)
maleelo_update$maledate = as.double(as.character(maleelo_update$maledate))
#list the elo scores for each individual before the race (prev_elo)
#upate the scores with the algorithm

upK=1
for(i in 1:max(unique(update_menxl$race))){
#for seeing how they compared x races ago
# for(i in 1:(max(unique(update_menxl$race))-1)){
#for(i in 1:1){
  print(i)
  cur_race = update_menxl[which(update_menxl$race==i), ]
  #print(cur_race)
  prev_elo = c()
  post_elo = c()
  for(j in 1:length(cur_race[,1])){
    #print(j)
    if(cur_race$name[j]%in%maleelo_update$malenames){
    prev_elo = append(prev_elo, maleelo_update[which(maleelo_update$malenames == cur_race$name[j]), 4])
    }
    else{
      prev_elo = append(prev_elo, 1300)
    }
  }
  cur_race$prev_elo = as.double(as.character(prev_elo))
  #for(j in 1:1){
  for(j in 1:length(cur_race[,1])){
    #print(j)
      win_places = which(as.double(as.character(cur_race$place))>as.double(as.character(cur_race$place[j])))
     # print(win_places)
      draw_places = which(as.double(as.character(cur_race$place))==as.double(as.character(cur_race$place[j])))
      loss_places = which(as.double(as.character(cur_race$place))<as.double(as.character(cur_race$place[j])))
      
      upr1 <- cur_race$prev_elo[j]
      #print(cur_race$prev_elo[j])
      #print(upr1)
      upR1 = 10^(upr1/400)
      if(length(win_places)>0){
        
        upwR2 = sum(10^(cur_race$prev_elo[win_places]/400))/length(win_places)
        upwE1 = upR1/(upR1+upwR2)
        upwS1 = length(win_places)
      }
      else{
        upwS1 = 0
        upwE1 = 0
      }
      if(length(draw_places>1)){
        updR2 = sum(10^(cur_race$prev_elo[draw_places]/400))/length(draw_places)
        updE1 = upR1/(upR1+updR2)
        updS1 = length(draw_places)-1
      }
      else{
        updE1 = 0
        updS1 = 0
      }
      if(length(loss_places>0)){
        uplR2 = sum(10^(cur_race$prev_elo[loss_places]/400))/length(loss_places)
        uplE1 = upR1/(upR1+uplR2)
        uplS1 = length(loss_places)
      }
      else{
        uplE1 = 0
        uplS1 = 0
      }
      
      upr11 = upr1+upwS1*upK*(1-upwE1)+updS1*upK*(.5-updE1)+uplS1*upK*(0-uplE1)
      post_elo = append(post_elo, upr11)
      
    
    }
  cur_race$post_elo = post_elo
  #cur_race$prev_elo = prev_elo
  for (k in 1:length(cur_race[,1])){
    name_num=which(maleelo_update$malenames==cur_race$name[k])
    if(length(name_num>0)){
    maleelo_update$maledate[name_num] = as.double(as.character(cur_race$date[k]))
    maleelo_update$maledistance[name_num] = cur_race$distance[k]
    maleelo_update$maleeloscore[name_num] = cur_race$post_elo[k]
    }
    else{
      newskier = c(cur_race$name[k], cur_race$date[k], cur_race$distance[k], cur_race$post_elo[k])
      maleelo_update = rbind(maleelo_update, newskier)
    }
  
  }
  
  
  }
  #maleelo_update = 
#  print(prev_elo)
  #cur_race$prev_elo = prev_elo
  
  #print(cur_race)
  
#}




race_num = c(1)
race_n = 1
for(a in 2:length(update_ladiesxl[,1])){
  if(as.double(as.character(update_ladiesxl$place[a]))>=as.double(as.character(update_ladiesxl$place[a-1]))){
    race_num = append(race_num, race_n)
  }
  else{
    #print(update_ladiesxl[a,])
    race_n = race_n+1
    race_num = append(race_num, race_n)
  }
}

update_ladiesxl$race = race_num
#list the elo scores for each individual before the race (prev_elo)
#upate the scores with the algorithm
ladyelo_update$ladynames = as.character(ladyelo_update$ladynames)
ladyelo_update$ladydate = as.double(as.character(ladyelo_update$ladydate))


upK=1
for(i in 1:max(unique(update_ladiesxl$race))){
#For seeing how they compared x races ago
#for(i in 1:max((unique(update_ladiesxl$race))-1)){
  #for(i in 1:1){
  print(i)
  cur_race = update_ladiesxl[which(update_ladiesxl$race==i), ]
  #print(cur_race)
  prev_elo = c()
  post_elo = c()
  for(j in 1:length(cur_race[,1])){
    #print(j)
    if(cur_race$name[j]%in%ladyelo_update$ladynames){
      prev_elo = append(prev_elo, ladyelo_update[which(ladyelo_update$ladynames == cur_race$name[j]), 4])
    }
    else{
      prev_elo = append(prev_elo, 1300)
    }
  }
  cur_race$prev_elo = as.double(as.character(prev_elo))
  #print(cur_race)
  
  #for(j in 1:1){
  for(j in 1:length(cur_race[,1])){
    #print(j)
    win_places = which(as.double(as.character(cur_race$place))>as.double(as.character(cur_race$place[j])))
    # print(win_places)
    draw_places = which(as.double(as.character(cur_race$place))==as.double(as.character(cur_race$place[j])))
    loss_places = which(as.double(as.character(cur_race$place))<as.double(as.character(cur_race$place[j])))
    
    upr1 <- as.double(as.character(cur_race$prev_elo[j]))
    #print(cur_race$prev_elo[j])
    #print(upr1)
    upR1 = 10^(upr1/400)
    if(length(win_places)>0){
      
      upwR2 = sum(10^(as.double(as.character(cur_race$prev_elo[win_places]))/400))/length(win_places)
      upwE1 = upR1/(upR1+upwR2)
      upwS1 = length(win_places)
    }
    else{
      upwS1 = 0
      upwE1 = 0
    }
    if(length(draw_places>1)){
      updR2 = sum(10^(cur_race$prev_elo[draw_places]/400))/length(draw_places)
      updE1 = upR1/(upR1+updR2)
      updS1 = length(draw_places)-1
    }
    else{
      updE1 = 0
      updS1 = 0
    }
    if(length(loss_places>0)){
      uplR2 = sum(10^(cur_race$prev_elo[loss_places]/400))/length(loss_places)
      uplE1 = upR1/(upR1+uplR2)
      uplS1 = length(loss_places)
    }
    else{
      uplE1 = 0
      uplS1 = 0
    }
    
    upr11 = upr1+upwS1*upK*(1-upwE1)+updS1*upK*(.5-updE1)+uplS1*upK*(0-uplE1)
    post_elo = append(post_elo, upr11)
    
    
  }
  cur_race$post_elo = post_elo
  #cur_race$prev_elo = prev_elo
  for (k in 1:length(cur_race[,1])){
    name_num=which(ladyelo_update$ladynames==cur_race$name[k])
    if(length(name_num>0)){
      #print(cur_race$date[k])
      ladyelo_update$ladydate[name_num] = as.double(as.character(cur_race$date[k]))
      ladyelo_update$ladydistance[name_num] = cur_race$distance[k]
      ladyelo_update$ladyeloscore[name_num] = cur_race$post_elo[k]
    }
    else{
      newskier = c(cur_race$name[k], cur_race$date[k], cur_race$distance[k], cur_race$post_elo[k])
      ladyelo_update = rbind(ladyelo_update, newskier)
      #print(newskier)
    }
  }
  
  
}

#print(cur_race[1:25,])
#maleelo_update[order(-maleelo_update$maleeloscore), ][1:25,]

maleupdate_standings <- (maleelo_update[order(-as.double(as.character(maleelo_update$maleeloscore))), ])
row.names(maleupdate_standings) = c(1:length(maleupdate_standings[,1]))
ladyupdate_standings <- (ladyelo_update[order(-as.double(as.character(ladyelo_update$ladyeloscore))), ])
row.names(ladyupdate_standings) = c(1:length(ladyupdate_standings[,1]))
print(maleupdate_standings[1:50,])
print(ladyupdate_standings[1:50,])

actmaleupdate_standings <- maleupdate_standings[which(maleupdate_standings$maledate!="20200500"), ]
row.names(actmaleupdate_standings) <- c(1:length(actmaleupdate_standings[,1]))
actladyupdate_standings <- ladyupdate_standings[which(ladyupdate_standings$ladydate!="20200500"), ]
row.names(actladyupdate_standings) <- c(1:length(actladyupdate_standings[,1]))
