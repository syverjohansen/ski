library(readxl)
library(fmsb)
library(ggplot2)
library(purrr)
library(iterators)
library(lmtest)

x <- read_excel("/Users/syverjohansen/ski/elo/python/ski/age/excel365/men_chrono_regress_sprint_classic.xlsx", 
                sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)

##Optional, but make pelopct the comparise against the alltime maximum
#max_elo = max(ladies$elo)
#ladies$pelopct = ladies$pelo/max_elo


df <- data.frame(x)
#df <- df[which(df$pelo>=80), ]
#df <- df[which(df$total_pelo!=1300)]
df <- df[which(df$season>=2019),]




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

summary(bestAIC)
summary(bestBIC)
summary(bestR2)

print("AIC")
paste(names(bestAIC$coefficients[2:length(names(bestAIC$coefficients))]), collapse= "', '")

print("BIC")
paste(names(bestBIC$coefficients[2:length(names(bestBIC$coefficients))]), collapse= "', '")

print("R2")
paste(names(bestR2$coefficients[2:length(names(bestR2$coefficients))]), collapse="', '")


########
#Read in startlist and make prediction based on that
x <- read_excel("/Users/syverjohansen/ski/elo/knapsack/excel365/startlist_men.xlsx", 
                sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)

df <- data.frame(x)
colnames(df)
df <- df[, c(2, 11, 12, 15, 16, 17, 19, 21, 23, 25, 27, 29, 31, 33, 40, 41)]
df$home = ifelse(df$nation=="Norway", TRUE,FALSE)
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

df<-df[, c(2, 3, 17, 18, 19)]
df
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






