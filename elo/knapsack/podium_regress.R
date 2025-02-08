library(readxl)
library(fmsb)
library(ggplot2)
library(purrr)
library(iterators)
library(lmtest)
library(VIM)

x <- read_excel("/Users/syverjohansen/ski/elo/python/ski/age/excel365/ladies_chrono_regress_distance_freestyle.xlsx", 
                sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)

##Optional, but make pelopct the comparise against the alltime maximum
#max_elo = max(ladies$elo)
#ladies$pelopct = ladies$pelo/max_elo


df <- data.frame(x)
#df <- df[which(df$pelo>=80), ]
#df <- df[which(df$total_pelo!=1300)]
df <- df[which(df$season>=2019),]
df[which(df$name == "Kristine Stavås Skistad"), ]
df_impute <- kNN(df,variable = colnames(df)[c(15, 16, 17, 19, 21, 23, 25, 27, 29, 31, 33, 40, 41)]  ,k=3)
df_impute[which(df_impute$name=="Kristine Stavås Skistad"), ]
df <- df_impute
colnames(df)

df_cols = names(df)
df_cols
df_cols = names(df[c(15, 16, 17, 19, 21, 23, 25, 27, 29, 31, 33, 40, 41)])
df$podium = ifelse(df$place<=1, 1,0)
whichcols <- Reduce("c", map(1:length(df_cols), ~lapply(iter(combn(df_cols,.x), by="col"),function(y) c(y))))
models <- map(1:length(whichcols), ~glm((podium) ~., data=df[c(whichcols[[.x]], "podium")], family = binomial))
#models.lm <- map(1:length(whichcols), ~lm((podium) ~., data=df[c(whichcols[[.x]], "podium")], family = binomial))
bestAIC <- models[[which.min(sapply(1:length(models),function(x)AIC(models[[x]])))]]
bestBIC <- models[[which.min(sapply(1:length(models),function(x)BIC(models[[x]])))]]
#bestR2 <- models.lm[[which.max(sapply(1:length(models.lm),function(x)summary(models.lm[[x]])$adj.r.squared))]]
bestELO <- glm(podium~distance_freestyle_pelo, family=binomial, data=df)
summary(bestAIC)
summary(bestBIC)
#summary(bestR2)

print("AIC")
paste(names(bestAIC$coefficients[2:length(names(bestAIC$coefficients))]), collapse= "', '")

print("BIC")
paste(names(bestBIC$coefficients[2:length(names(bestBIC$coefficients))]), collapse= "', '")

#print("R2")
#paste(names(bestR2$coefficients[2:length(names(bestR2$coefficients))]), collapse="', '")

df$predictAIC = predict(bestAIC, df, type="response")
df$predictBIC = predict(bestBIC, df, type="response")
df$predictELO = predict(bestELO, df, type="response")
#df$predictR2 = predict(bestR2, df, type="response")
df_wins = df[which(df$podium==1), ]
df_wins$name
plot(df_wins$date, df_wins$predictAIC)
df_wins[,c(3,10,59,60,61)]

df_wins[which(df_wins$predictAIC==min(df_wins$predict_AIC, na.rm=TRUE)), ]










########
#Read in startlist and make prediction based on that
x <- read_excel("/Users/syverjohansen/ski/elo/knapsack/excel365/startlist_men.xlsx", 
                sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)

df <- data.frame(x)
colnames(df)
df <- df[, c(2, 11, 12, 15, 16, 17, 19, 21, 23, 25, 27, 29, 31, 33, 40, 41)]
df$home = ifelse(df$nation=="Switzerland", TRUE,FALSE)
df$age[df$age==0] <- median(df$age)


for(i in 6:ncol(df)){
  if(is.numeric(df[[i]])){
    na <- is.na(df[[i]])
    df[na, i] <- quantile(df[[i]], .20, na.rm=TRUE)[[1]]
  }
}

#Prediction time!
summary(bestAIC)
summary(bestBIC)
summary(bestR2)
df$predAIC = predict(bestAIC, df, type='response')
df$predBIC = predict(bestBIC, df, type='response')
df$predR2 = predict(bestR2, df, type='response')

df <- df[order(-df$predAIC), ]
plot(df$predAIC)
points(df$predBIC, col=2)
points(df$predR2, col=3)

show_df<-df[, c(2, 3, 17, 18, 19)]
df



#####
#Number of athletes on podium for Tour de Ski
#1) Get a Df of all the races.  DFAIC, DFBIC, DFR2.  The races are SF, DC, DF, SF, DF, DC, DF

##
#The World Cup point system rewards athletes with allround qualities, 
#who manage to ski well in sprint as well as in races over longer distances. 
#Therefore our question for this week is: How many of the athletes 
#on Saturday's sprint podium will be able to secure a second podium position in Sunday's 20K?
x <- read_excel("/Users/syverjohansen/ski/elo/python/ski/age/excel365/men_chrono_regress_sprint_freestyle.xlsx", 
                sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)
x2 <- read_excel("/Users/syverjohansen/ski/elo/python/ski/age/excel365/men_chrono_regress_distance_freestyle.xlsx", 
                 sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)
df <- data.frame(x)
df <- df[which(df$season>=2019),]

df2 <- data.frame(x2)
df2 <- df2[which(df2$season>=2019),]

df_cols = names(df)
df_cols
df_cols = names(df[c(15, 16, 17, 19, 21, 23, 25, 27, 29, 31, 33, 40, 41)])
df$podium = ifelse(df$place<=3, 1,0)
whichcols <- Reduce("c", map(1:length(df_cols), ~lapply(iter(combn(df_cols,.x), by="col"),function(y) c(y))))
models <- map(1:length(whichcols), ~glm((podium) ~., data=df[c(whichcols[[.x]], "podium")], family = binomial))
models.lm <- map(1:length(whichcols), ~lm((podium) ~., data=df[c(whichcols[[.x]], "podium")], family = binomial))
bestAIC <- models[[which.min(sapply(1:length(models),function(x)AIC(models[[x]])))]]
bestBIC <- models[[which.min(sapply(1:length(models),function(x)BIC(models[[x]])))]]
bestR2 <- models.lm[[which.max(sapply(1:length(models.lm),function(x)summary(models.lm[[x]])$adj.r.squared))]]


print("AIC")
paste(names(bestAIC$coefficients[2:length(names(bestAIC$coefficients))]), collapse= "', '")

print("BIC")
paste(names(bestBIC$coefficients[2:length(names(bestBIC$coefficients))]), collapse= "', '")

print("R2")
paste(names(bestR2$coefficients[2:length(names(bestR2$coefficients))]), collapse="', '")

df2_cols = names(df2)
df2_cols
df2_cols = names(df2[c(15, 16, 17, 19, 21, 23, 25, 27, 29, 31, 33, 40, 41)])
df2$podium = ifelse(df2$place<=3, 1,0)
whichcols2 <- Reduce("c", map(1:length(df2_cols), ~lapply(iter(combn(df2_cols,.x), by="col"),function(y) c(y))))
models2 <- map(1:length(whichcols2), ~glm((podium) ~., data=df2[c(whichcols2[[.x]], "podium")], family = binomial))
models.lm2 <- map(1:length(whichcols2), ~lm((podium) ~., data=df2[c(whichcols2[[.x]], "podium")], family = binomial))
bestAIC2 <- models2[[which.min(sapply(1:length(models2),function(x)AIC(models2[[x]])))]]
bestBIC2 <- models2[[which.min(sapply(1:length(models2),function(x)BIC(models2[[x]])))]]
bestR22 <- models.lm2[[which.max(sapply(1:length(models.lm2),function(x)summary(models.lm2[[x]])$adj.r.squared))]]


print("AIC")
paste(names(bestAIC2$coefficients[2:length(names(bestAIC2$coefficients))]), collapse= "', '")

print("BIC")
paste(names(bestBIC2$coefficients[2:length(names(bestBIC2$coefficients))]), collapse= "', '")

print("R2")
paste(names(bestR22$coefficients[2:length(names(bestR22$coefficients))]), collapse="', '")

x <- read_excel("/Users/syverjohansen/ski/elo/knapsack/excel365/startlist_men.xlsx", 
                sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)

df <- data.frame(x)
colnames(df)
df <- df[, c(2, 11, 12, 15, 16, 17, 19, 21, 23, 25, 27, 29, 31, 33, 40, 41)]
df$home = ifelse(df$nation=="Switzerland", TRUE,FALSE)
df$age[df$age==0] <- median(df$age)


for(i in 6:ncol(df)){
  if(is.numeric(df[[i]])){
    na <- is.na(df[[i]])
    df[na, i] <- quantile(df[[i]], .20, na.rm=TRUE)[[1]]
  }
}

#Prediction time!
summary(bestAIC)
summary(bestBIC)
summary(bestR2)
df$predAIC = predict(bestAIC, df, type='response')
df$predBIC = predict(bestBIC, df, type='response')
df$predR2 = predict(bestR2, df, type='response')

df <- df[order(df$id), ]
plot(df$predAIC)
points(df$predBIC, col=2)
points(df$predR2, col=3)

show_df<-df[, c(2, 3, 17, 18, 19)]

x2 <- read_excel("/Users/syverjohansen/ski/elo/knapsack/excel365/startlist_men.xlsx", 
                sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)

df2 <- data.frame(x2)
colnames(df2)
df2 <- df2[, c(2, 11, 12, 15, 16, 17, 19, 21, 23, 25, 27, 29, 31, 33, 40, 41)]
df2$home = ifelse(df2$nation=="Switzerland", TRUE,FALSE)
df2$age[df2$age==0] <- median(df2$age)


for(i in 6:ncol(df2)){
  if(is.numeric(df2[[i]])){
    na <- is.na(df2[[i]])
    df2[na, i] <- quantile(df2[[i]], .20, na.rm=TRUE)[[1]]
  }
}

#Prediction time!
df2$predAIC = predict(bestAIC2, df, type='response')
df2$predBIC = predict(bestBIC2, df, type='response')
df2$predR2 = predict(bestR22, df, type='response')

df2 <- df2[order(df2$id), ]
plot(df2$predAIC)
points(df2$predBIC, col=2)
points(df2$predR2, col=3)

show_df2<-df2[, c(2, 3, 17, 18, 19)]

two_podium = df$predBIC*df2$predBIC
two_podium_vector = c(two_podium)
two_podium_vector = two_podium_vector[!is.na(two_podium_vector)]
#models.lm <- map(1:length(whichcols), ~lm((podium) ~., data=df[c(whichcols[[.x]], "podium")], family = binomial))




#Exploratory Data Analysis
############
#AIC
############
summary(bestAIC)
confint(bestAIC)
#predAIC = (predict(bestAIC))
predAIC = predict(bestAIC, type='response')
deparse(predAIC) 
attributes_predAIC = as.double(attributes(predAIC)$names)

dfAIC = data.frame(x)
dfAIC$podium = ifelse(dfAIC$place<=3, 1,0)
dfAIC = dfAIC[c(attributes_predAIC), ]
dfAIC$predAIC = predAIC
most_recent = dfAIC[which(dfAIC$date==max(dfAIC$date)), ]
this_season = dfAIC[which(dfAIC$season==max(dfAIC$season)), ]
plot(this_season$predAIC, this_season$podium)

data.frame(most_recent$name, most_recent$predAIC)

xx = predAIC
yy=dfAIC$podium
par(mfrow=c(1,1))
plot(predAIC,yy)
par(mfrow=c(2,2))
plot(bestAIC)  
bptest(bestAIC)

pod.pred = rep("on", length(dfAIC$podium))
pod.pred[predAIC<.66] = "off"
freq_table = table(pod.pred, dfAIC$podium)
freq_table
freq_table[1,1]
tp = freq_table[2,2]
tn = freq_table[1,1]
fp = freq_table[2,1]
fn = freq_table[1,2]

tpr = tp/(tp+fn)
tnr = tn/(tn+fp)

dfAIC






