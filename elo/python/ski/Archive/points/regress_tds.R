library(readxl)
library(fmsb)
library(ggplot2)
library(relaimpo)
library(MASS)


men_all <- data.frame(read_excel("/Users/syverjohansen/ski/elo/python/ski/points/varmen_all_k_tds.xlsx", 
                       sheet = "Sheet1", col_names = TRUE, na = "NA"))
men_distance <- data.frame(read_excel("/Users/syverjohansen/ski/elo/python/ski/points/varmen_distance_k_tds.xlsx", 
                         sheet = "Sheet1", col_names = TRUE, na = "NA"))
men_distance_freestyle <- data.frame(read_excel("/Users/syverjohansen/ski/elo/python/ski/points/varmen_distance_freestyle_k_tds.xlsx", 
                         sheet = "Sheet1", col_names = TRUE, na = "NA"))
men_distance_classic <- data.frame(read_excel("/Users/syverjohansen/ski/elo/python/ski/points/varmen_distance_classic_k_tds.xlsx", 
                         sheet = "Sheet1", col_names = TRUE, na = "NA"))
men_sprint <- data.frame(read_excel("/Users/syverjohansen/ski/elo/python/ski/points/varmen_sprint_k_tds.xlsx", 
                         sheet = "Sheet1", col_names = TRUE, na = "NA"))
men_sprint_freestyle <- data.frame(read_excel("/Users/syverjohansen/ski/elo/python/ski/points/varmen_sprint_freestyle_k_tds.xlsx", 
                                      sheet = "Sheet1", col_names = TRUE, na = "NA"))
men_sprint_classic <- data.frame(read_excel("/Users/syverjohansen/ski/elo/python/ski/points/varmen_sprint_classic_k_tds.xlsx", 
                         sheet = "Sheet1", col_names = TRUE, na = "NA"))


men <- men_all[, c("name", "season", "points", "pelopct")]
colnames(men)[4] = "pelopct_all" 
men$pelopct_distance = men_distance$pelopct
men$pelopct_distance_freestyle = men_distance_freestyle$pelopct
men$pelopct_distance_classic = men_distance_classic$pelopct
men$pelopct_sprint = men_sprint$pelopct
men$pelopct_sprint_freestyle = men_sprint_freestyle$pelopct
men$pelopct_sprint_classic = men_sprint_classic$pelopct


#men <- men[which(men$season>=2018),]
#men <- men[which(men$pavg_points>=10), ]
special_men <- men[which(men$name == "Maja Dahlqvist"), ]
special_men
plot(special_men$points)
hist(special_men$points)

ggplot(men, aes(x=pelopct_distance, y=points)) + geom_point()
men.lm <- lm((points) ~ pelopct_distance, data=men)#, family="gaussian")
summary(men.lm)


ggplot(men, aes(x=pelopct_distance, y=(points)^(1/exp(1)))) + geom_point()
men.lm <- lm((points)^(1/exp(1)) ~  pelopct_distance, data=men)
#men.lm <- lm((points)^(1/1) ~ as.numeric(pavg_points^(1/1)) + pelopct, data=men)
summary(men.lm)
x = (men.lm$coefficients[[1]] + (85.87*men.lm$coefficients[[2]])^(1/exp(1)) + 100*men.lm$coefficients[[3]])^exp(1)
x = -7.984109 + .282276*(80^(1/exp(1))) + .108524*100
x^(exp(1))

anova(men.lm)
confint(men.lm)

layout(matrix(c(1,2,3,4),2,2)) # optional 4 graphs/page
plot(men.lm)

calc.relimp(men.lm,type=c("lmg","last","first","pratt"),
            rela=TRUE)
boot <- boot.relimp(men.lm, b = 1000, type = c("lmg",
                                                  "last", "first", "pratt"), rank = TRUE,
                    diff = TRUE, rela = TRUE)
booteval.relimp(boot) # print result
plot(booteval.relimp(boot,sort=Fa)) # plot result

newstuff = data.frame(pavg_points = 80, pelopct=100)
predict(men.lm, newstuff)

men$prediction = predict(men.lm, men)^exp(1)






#######Find best model#############
fit <- lm((points)^(1/exp(1)) ~ pelopct_all+pelopct_distance+pelopct_distance_classic+
            pelopct_distance_freestyle+pelopct_sprint+pelopct_sprint_classic+
            pelopct_sprint_freestyle, data=men)
step <- stepAIC(fit, direction="both")

step$anova
men.lm <- lm((points)^(1/exp(1)) ~  pelopct_all + pelopct_distance_classic+pelopct_distance_freestyle+
                  pelopct_sprint_classic+pelopct_sprint_freestyle, data=men)

men.lm <- men.lm <- lm((points)^(1/exp(1)) ~  pelopct_all + 
                         pelopct_sprint_freestyle, data=men)

men.lm <- lm((points)^(1/exp(1)) ~  pelopct_distance+pelopct_sprint, data=men)
summary(men.lm)

