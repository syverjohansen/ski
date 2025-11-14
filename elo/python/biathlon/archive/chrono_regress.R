#install.packages("reticulate")


library(readxl)
library(fmsb)
library(ggplot2)
library(purrr)
library(iterators)
library(lmtest)

inverse = function(fn, interval = NULL, lower = min(interval), upper = max(interval), ...){
  Vectorize(function(y){
    uniroot(f=function(x){fn(x)-y}, lower=lower, upper=upper, ...)$root
  })
}

linearize_points = function(points){
  linear_points = -0.000000014134*points^5+0.000004410550*points^4-0.000514828250*points^3+0.030918434213*points^2-1.451289105497*points+52.299164352385
  return(linear_points)
}


x <- read_excel("/Users/syverjohansen/ski/elo/python/ski/age/excel365/ladies_chrono_regress_distance_classic.xlsx", 
                       sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)

##Optional, but make pelopct the comparise against the alltime maximum
#max_elo = max(ladies$elo)
#ladies$pelopct = ladies$pelo/max_elo


df <- data.frame(x)
#df <- df[which(df$pelo>=80), ]
#df <- df[which(df$total_pelo!=1300)]




df_cols = names(df)
df_cols
df_cols = names(df[c(15, 16, 17, 19, 21, 23, 25, 27, 29, 31, 33, 40, 41)])


#Change it to places

df_curve = data.frame(y=50:1, x=c(100,95,90,85,80,75,72,69,66,63,60,58,56,54,52,50,48,46,44,42,40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1))

#df_curve = data.frame(y=50:1, x=c(300, 285, 270, 255, 240, 225, 216, 207, 198, 189, 180, 174, 168, 162, 156, 150, 144, 138, 132, 126, 120, 114, 108, 102, 96, 90, 84, 78, 72, 66, 60, 57, 54, 51, 48, 45, 42, 29, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3))
#df_curve = data.frame(y=30:1, x=c(50, 47, 44, 41, 38, 35, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1))

plot(df_curve$x, df_curve$y)
fit1 <- lm(y~x, data=df_curve)
fit2 <- lm(y~poly(x,2,raw=TRUE), data=df_curve)
fit3 <- lm(y~poly(x,3,raw=TRUE), data=df_curve)
fit4 <- lm(y~poly(x,4,raw=TRUE), data=df_curve)
fit5 <- lm(y~poly(x,5,raw=TRUE), data=df_curve)

plot(df_curve$x, df_curve$y)
x_axis = seq(1,50, length=50)
lines(x_axis, predict(fit1, data.frame(x=x_axis)), col='green')
lines(x_axis, predict(fit2, data.frame(x=x_axis)), col='red')
lines(x_axis, predict(fit3, data.frame(x=x_axis)), col='purple')
lines(x_axis, predict(fit4, data.frame(x=x_axis)), col='blue')
lines(x_axis, predict(fit5, data.frame(x=x_axis)), col='orange')

summary(fit1)$adj.r.squared
summary(fit2)$adj.r.squared
summary(fit3)$adj.r.squared
summary(fit4)$adj.r.squared
summary(fit5)$adj.r.squared
summary(fit2)

#Individual
df$points =   1.5865602+.896294*df$points-0.0042397*df$points^2
df$pavg_points =   1.5865602+.896294*df$pavg_points-0.0042397*df$pavg_points^2

df20 <- df[which(df$season>=2003),]
df15 <- df[which(df$season>=2008),]
df10 <- df[which(df$season>=2013),]
df5 <- df[which(df$season>=2018),]
df4 <- df[which(df$season>=2019),]
df3 <- df[which(df$season>=2020),]
df2 <- df[which(df$season>=2021),]
df1 <- df[which(df$season>=2022),]
df0 <- df[which(df$season>=2023),]
df <- df[which(df$season>=2018),]

#Stage
#df$points =   0.1584908+1.0801595*df$points-0.0100871*df$points^2
#df$pavg_points =   0.1584908+1.0801595*df$pavg_points-0.0100871*df$pavg_points^2

#Tour de Ski
#df$points = df$points*3
#df$points = 1.76564661 + 0.29674200*df$points-0.00046598*df$points^2
#df$pavg_points = 1.76564661 + 0.29674200*df$pavg_points-0.00046598*df$pavg_points^2



options(scipen=999)



# whichcols5 <- Reduce("c", map(1:length(df_cols), ~lapply(iter(combn(df_cols,.x), by="col"),function(y) c(y))))
# models5 <- map(1:length(whichcols5), ~glm((points) ~., data=df5[c(whichcols5[[.x]], "points")], family = gaussian))
# models.lm5 <- map(1:length(whichcols5), ~lm((points) ~., data=df5[c(whichcols5[[.x]], "points")], family = gaussian))
# bestAIC5 <- models5[[which.min(sapply(1:length(models5),function(x)AIC(models5[[x]])))]]
# bestBIC5 <- models5[[which.min(sapply(1:length(models5),function(x)BIC(models5[[x]])))]]
# bestR25 <- models.lm5[[which.max(sapply(1:length(models.lm5),function(x)summary(models.lm5[[x]])$adj.r.squared))]]
# summary(bestAIC5)
# summary(bestBIC5)
# summary(bestR25)

whichcols <- Reduce("c", map(1:length(df_cols), ~lapply(iter(combn(df_cols,.x), by="col"),function(y) c(y))))
models <- map(1:length(whichcols), ~glm((points) ~., data=df[c(whichcols[[.x]], "points")], family = gaussian))
models.lm <- map(1:length(whichcols), ~lm((points) ~., data=df[c(whichcols[[.x]], "points")], family = gaussian))
bestAIC <- models[[which.min(sapply(1:length(models),function(x)AIC(models[[x]])))]]
bestBIC <- models[[which.min(sapply(1:length(models),function(x)BIC(models[[x]])))]]
bestR2 <- models.lm[[which.max(sapply(1:length(models.lm),function(x)summary(models.lm[[x]])$adj.r.squared))]]
summary(bestAIC)
summary(bestBIC)
summary(bestR2)

# whichcols2 <- Reduce("c", map(1:length(df_cols), ~lapply(iter(combn(df_cols,.x), by="col"),function(y) c(y))))
# models2 <- map(1:length(whichcols2), ~glm((points) ~., data=df2[c(whichcols2[[.x]], "points")], family = gaussian))
# models.lm2 <- map(1:length(whichcols2), ~lm((points) ~., data=df2[c(whichcols2[[.x]], "points")], family = gaussian))
# bestAIC2 <- models2[[which.min(sapply(1:length(models2),function(x)AIC(models2[[x]])))]]
# bestBIC2 <- models2[[which.min(sapply(1:length(models2),function(x)BIC(models2[[x]])))]]
# bestR22 <- models.lm2[[which.max(sapply(1:length(models.lm2),function(x)summary(models.lm2[[x]])$adj.r.squared))]]
# summary(bestAIC2)
# summary(bestBIC2)
# summary(bestR22)
# #AIC_model_list = models[order(sapply(1:length(models),function(x)AIC(models[[x]])), decreasing = FALSE)]
# #AIC_model_list[1]
# #bestAIC
# #bestBIC
# summary(bestAIC)
# summary(bestBIC)

summary(bestR2)


print("AIC")
paste(names(bestAIC$coefficients[2:length(names(bestAIC$coefficients))]), collapse= "', '")

print("BIC")
paste(names(bestBIC$coefficients[2:length(names(bestBIC$coefficients))]), collapse= "', '")

print("R2")
paste(names(bestR2$coefficients[2:length(names(bestR2$coefficients))]), collapse="', '")


#Change it back to points
#df_curve = data.frame(y=50:1, x=c(100,95,90,85,80,75,72,69,66,63,60,58,56,54,52,50,48,46,44,42,40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1))
df_curve = data.frame(x=50:1, y=c(100,95,90,85,80,75,72,69,66,63,60,58,56,54,52,50,48,46,44,42,40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1))

#df_curve = data.frame(x=50:1, y=c(300, 285, 270, 255, 240, 225, 216, 207, 198, 189, 180, 174, 168, 162, 156, 150, 144, 138, 132, 126, 120, 114, 108, 102, 96, 90, 84, 78, 72, 66, 60, 57, 54, 51, 48, 45, 42, 29, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3))
#df_curve = data.frame(x=30:1, y=c(50, 47, 44, 41, 38, 35, 32, 30, 28, 26, 24, 22, 20, 18, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1))


plot(df_curve$x, df_curve$y)
fit2 <- lm(y~poly(x,2,raw=TRUE), data=df_curve)
plot(df_curve$x, df_curve$y)
x_axis = seq(1,50, length=50)
lines(x_axis, predict(fit1, data.frame(x=x_axis)), col='green')
lines(x_axis, predict(fit2, data.frame(x=x_axis)), col='red')
summary(fit2)


##Exploratory data analysis
summary(bestAIC)
confint(bestAIC)
predAIC = (predict(bestAIC))
predAIC = 3.196429+0.263557*predAIC+ 0.030964*predAIC^2

deparse(predAIC) 
attributes_predAIC = as.double(attributes(predAIC)$names)

dfAIC = data.frame(x)  
dfAIC = dfAIC[c(attributes_predAIC), ]
xx = predAIC
yy=dfAIC$points
plot(xx,yy)
fit1 <- lm(yy~xx)
fit2 <- lm(yy~poly(xx,2,raw=TRUE))
fit3 <- lm(yy~poly(xx,3,raw=TRUE))
fit4 <- lm(yy~poly(xx,4,raw=TRUE))
fit5 <- lm(yy~poly(xx,5,raw=TRUE))
fit.sq <- lm(yy~sqrt(xx))
fit.log <- lm(yy~log(xx))

x_axis = seq(min(predAIC), max(predAIC), length.out=length(predAIC))
line1 = fit1$coefficients[1]+fit1$coefficients[2]*x_axis
line2 = fit2$coefficients[1]+fit2$coefficients[2]*x_axis+fit2$coefficients[3]*x_axis
line3 = fit3$coefficients[1]+fit3$coefficients[2]*x_axis+fit3$coefficients[3]*x_axis+
  +fit3$coefficients[4]*x_axis
line4 = fit4$coefficients[1]+fit4$coefficients[2]*x_axis+fit4$coefficients[3]*x_axis+
  +fit4$coefficients[4]*x_axis+fit4$coefficients[5]*x_axis
line5 = fit5$coefficients[1]+fit5$coefficients[2]*x_axis+fit5$coefficients[3]*x_axis+
  +fit5$coefficients[4]*x_axis+fit5$coefficients[5]*x_axis+fit5$coefficients[6]*x_axis

line6 = fit.sq$coefficients[1]+fit.sq$coefficients[2]*x_axis
line7 = fit.log$coefficients[1]+fit.log$coefficients[2]*x_axis


lines(x_axis, line1, col='green')
lines(x_axis, line2, col='red')
lines(x_axis, line3, col='purple')
lines(x_axis, line4, col='blue')
lines(x_axis, line5, col='orange')
lines(x_axis, line6, col='turquoise')
lines(x_axis, line7, col='turquoise')


summary(fit1)$adj.r.squared
summary(fit2)$adj.r.squared
summary(fit3)$adj.r.squared
summary(fit4)$adj.r.squared
summary(fit5)$adj.r.squared
summary(fit.sq)$adj.r.squared
summary(fit.log)$adj.r.squared
summary(fit2)

par(mfrow=c(2,2))
plot(bestAIC)  

bptest(bestAIC)

########
#BIC#
#######
summary(bestBIC)
confint(bestBIC)
predBIC = (predict(bestBIC))
predBIC = 3.196429+0.263557*predBIC+ 0.030964*predBIC^2

deparse(predBIC) 
attributes_predBIC = as.double(attributes(predBIC)$names)

dfBIC = data.frame(x)  
dfBIC = dfBIC[c(attributes_predBIC), ]
xx = predBIC
yy=dfBIC$points
plot(xx,yy)
fit1 <- lm(yy~xx)
fit2 <- lm(yy~poly(xx,2,raw=TRUE))
fit3 <- lm(yy~poly(xx,3,raw=TRUE))
fit4 <- lm(yy~poly(xx,4,raw=TRUE))
fit5 <- lm(yy~poly(xx,5,raw=TRUE))
fit.sq <- lm(yy~sqrt(xx))

x_axis = seq(min(predBIC), max(predBIC), length.out=length(predBIC))
line1 = fit1$coefficients[1]+fit1$coefficients[2]*x_axis
line2 = fit2$coefficients[1]+fit2$coefficients[2]*x_axis+fit2$coefficients[3]*x_axis
line3 = fit3$coefficients[1]+fit3$coefficients[2]*x_axis+fit3$coefficients[3]*x_axis+
  +fit3$coefficients[4]*x_axis
line4 = fit4$coefficients[1]+fit4$coefficients[2]*x_axis+fit4$coefficients[3]*x_axis+
  +fit4$coefficients[4]*x_axis+fit4$coefficients[5]*x_axis
line5 = fit5$coefficients[1]+fit5$coefficients[2]*x_axis+fit5$coefficients[3]*x_axis+
  +fit5$coefficients[4]*x_axis+fit5$coefficients[5]*x_axis+fit5$coefficients[6]*x_axis

line6 = fit.sq$coefficients[1]+fit.sq$coefficients[2]*x_axis



lines(x_axis, line1, col='green')
lines(x_axis, line2, col='red')
lines(x_axis, line3, col='purple')
lines(x_axis, line4, col='blue')
lines(x_axis, line5, col='orange')
lines(x_axis, line6, col='turquoise')


summary(fit1)$adj.r.squared
summary(fit2)$adj.r.squared
summary(fit3)$adj.r.squared
summary(fit4)$adj.r.squared
summary(fit5)$adj.r.squared
summary(fit.sq)$adj.r.squared
summary(fit2)

par(mfrow=c(2,2))
plot(bestBIC)  

bptest(bestBIC)


#######
#R2
#######
summary(bestR2)
confint(bestR2)
predR2 = (predict(bestR2))
predR2 = 3.196429+0.263557*predR2+ 0.030964*predR2^2

deparse(predR2) 
attributes_predR2 = as.double(attributes(predR2)$names)

dfR2 = data.frame(x)  
dfR2 = dfR2[c(attributes_predR2), ]
xx = predR2
yy=dfR2$points
plot(xx,yy)
fit1 <- lm(yy~xx)
fit2 <- lm(yy~poly(xx,2,raw=TRUE))
fit3 <- lm(yy~poly(xx,3,raw=TRUE))
fit4 <- lm(yy~poly(xx,4,raw=TRUE))
fit5 <- lm(yy~poly(xx,5,raw=TRUE))
fit.sq <- lm(yy~sqrt(xx))

x_axis = seq(min(predR2), max(predR2), length.out=length(predR2))
line1 = fit1$coefficients[1]+fit1$coefficients[2]*x_axis
line2 = fit2$coefficients[1]+fit2$coefficients[2]*x_axis+fit2$coefficients[3]*x_axis
line3 = fit3$coefficients[1]+fit3$coefficients[2]*x_axis+fit3$coefficients[3]*x_axis+
  +fit3$coefficients[4]*x_axis
line4 = fit4$coefficients[1]+fit4$coefficients[2]*x_axis+fit4$coefficients[3]*x_axis+
  +fit4$coefficients[4]*x_axis+fit4$coefficients[5]*x_axis
line5 = fit5$coefficients[1]+fit5$coefficients[2]*x_axis+fit5$coefficients[3]*x_axis+
  +fit5$coefficients[4]*x_axis+fit5$coefficients[5]*x_axis+fit5$coefficients[6]*x_axis

line6 = fit.sq$coefficients[1]+fit.sq$coefficients[2]*x_axis



lines(x_axis, line1, col='green')
lines(x_axis, line2, col='red')
lines(x_axis, line3, col='purple')
lines(x_axis, line4, col='blue')
lines(x_axis, line5, col='orange')
lines(x_axis, line6, col='turquoise')


summary(fit1)$adj.r.squared
summary(fit2)$adj.r.squared
summary(fit3)$adj.r.squared
summary(fit4)$adj.r.squared
summary(fit5)$adj.r.squared
summary(fit.sq)$adj.r.squared
summary(fit2)

par(mfrow=c(2,2))
plot(bestR2)  

bptest(bestR2)
