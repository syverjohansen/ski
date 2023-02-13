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


x <- read_excel("/Users/syverjohansen/ski/elo/python/ski/age/relay/excel365/ladies_chrono_regress_relay.xlsx", 
                       sheet = "Sheet1", col_names = TRUE, na = "NA", guess_max = 100000)

##Optional, but make pelopct the comparise against the alltime maximum
#max_elo = max(ladies$elo)
#ladies$pelopct = ladies$pelo/max_elo


df <- data.frame(x)
#df <- df[which(df$pelo>=80), ]
#df <- df[which(df$total_pelo!=1300)]
df <- df[which(df$season>=2010),]




df_cols = names(df)
df_cols
df_cols = names(df[c(15, 16, 17, 19, 21, 23, 25, 27, 29, 31, 33, 40)])


#Change it to places

df_curve = data.frame(y=30:1, x=c(200, 160, 120, 100, 90, 80, 72, 64, 58, 52, 48, 44, 40, 36, 32, 30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2))


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
summary(fit3)
summary(fit4)

#Relay
df$points = -.958832685276+.682250876756*df$points-.006352194332*df$points^2+0.000027600716*df$points^3-0.000000045133*df$points^4
df$pavg_points =   -.958832685276+.682250876756*df$pavg_points-.006352194332*df$pavg_points^2+0.000027600716*df$pavg_points^3-0.000000045133*df$pavg_points^4





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

df_curve = data.frame(x=30:1, y=c(200, 160, 120, 100, 90, 80, 72, 64, 58, 52, 48, 44, 40, 36, 32, 30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2))




plot(df_curve$x, df_curve$y)
fit2 <- lm(y~poly(x,2,raw=TRUE), data=df_curve)
fit3 <- lm(y~poly(x,3,raw=TRUE), data=df_curve)
fit4 <- lm(y~poly(x,4,raw=TRUE), data=df_curve)
plot(df_curve$x, df_curve$y)
x_axis = seq(1,50, length=50)
lines(x_axis, predict(fit1, data.frame(x=x_axis)), col='green')
lines(x_axis, predict(fit2, data.frame(x=x_axis)), col='red')
summary(fit2)
summary(fit3)
summary(fit4)



