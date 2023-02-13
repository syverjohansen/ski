#install.packages("reticulate")


library(readxl)
library(fmsb)
library(ggplot2)
library(purrr)
library(iterators)

inverse = function(fn, interval = NULL, lower = min(interval), upper = max(interval), ...){
  Vectorize(function(y){
    uniroot(f=function(x){fn(x)-y}, lower=lower, upper=upper, ...)$root
  })
}

linearize_points = function(points){
  linear_points = -0.000000014134*points^5+0.000004410550*points^4-0.000514828250*points^3+0.030918434213*points^2-1.451289105497*points+52.299164352385
  return(linear_points)
}


x <- read_excel("/Users/syverjohansen/ski/elo/python/ski/age/excel365/ladies_chrono_regress_distance_freestyle.xlsx", 
                       sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)

##Optional, but make pelopct the comparise against the alltime maximum
#max_elo = max(ladies$elo)
#ladies$pelopct = ladies$pelo/max_elo


df <- data.frame(x)
#df <- df[which(df$pelo>=80), ]
#df <- df[which(df$total_pelo!=1300)]
df <- df[which(df$season>=2018),]




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

#Stage
#df$points =   0.1584908+1.0801595*df$points-0.0100871*df$points^2
#df$pavg_points =   0.1584908+1.0801595*df$pavg_points-0.0100871*df$pavg_points^2

#Tour de Ski
#df$points = df$points*3
#df$points = 1.76564661 + 0.29674200*df$points-0.00046598*df$points^2
#df$pavg_points = 1.76564661 + 0.29674200*df$pavg_points-0.00046598*df$pavg_points^2



options(scipen=999)




whichcols <- Reduce("c", map(1:length(df_cols), ~lapply(iter(combn(df_cols,.x), by="col"),function(y) c(y))))
#models <- map(1:length(whichcols), ~glm((points)^(1/exp(1)) ~., data=df[c(whichcols[[.x]], "points")], family = gaussian))
models <- map(1:length(whichcols), ~glm((points) ~., data=df[c(whichcols[[.x]], "points")], family = gaussian))
models.lm <- map(1:length(whichcols), ~lm((points) ~., data=df[c(whichcols[[.x]], "points")], family = gaussian))
bestAIC <- models[[which.min(sapply(1:length(models),function(x)AIC(models[[x]])))]]
bestBIC <- models[[which.min(sapply(1:length(models),function(x)BIC(models[[x]])))]]
bestR2 <- models.lm[[which.max(sapply(1:length(models.lm),function(x)summary(models.lm[[x]])$adj.r.squared))]]
#AIC_model_list = models[order(sapply(1:length(models),function(x)AIC(models[[x]])), decreasing = FALSE)]
#AIC_model_list[1]
#bestAIC
#bestBIC
summary(bestAIC)
summary(bestBIC)

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



