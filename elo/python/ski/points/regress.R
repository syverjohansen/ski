library(readxl)
library(fmsb)
library(ggplot2)
library(relaimpo)


ladies_x <- read_excel("/Users/syverjohansen/ski/elo/python/ski/points/varmen_sprint_freestyle_k.xlsx", 
                       sheet = "Sheet1", col_names = TRUE, na = "NA")

ladies <- data.frame(ladies_x)
ladies <- ladies[which(ladies$season>=2018),]
#ladies <- ladies[which(ladies$pavg_points>=10), ]
special_ladies <- ladies[which(ladies$name == "Maja Dahlqvist"), ]
special_ladies
plot(special_ladies$points)
hist(special_ladies$points)

ggplot(ladies, aes(x=pavg_points, y=points)) + geom_point()
ladies.lm <- lm((points) ~ ((pavg_points)), data=ladies)#, family="gaussian")
summary(ladies.lm)


ladies.lm <- lm((points)^(1/exp(1)) ~ as.numeric(pavg_points^(1/exp(1))) + pelopct, data=ladies)
ladies.lm <- lm((points)^(1/1) ~ as.numeric(pavg_points^(1/1)) + pelopct, data=ladies)
summary(ladies.lm)
x = (ladies.lm$coefficients[[1]] + (85.87*ladies.lm$coefficients[[2]])^(1/exp(1)) + 100*ladies.lm$coefficients[[3]])^exp(1)
x = -7.984109 + .282276*(80^(1/exp(1))) + .108524*100
x^(exp(1))

anova(ladies.lm)
confint(ladies.lm)

layout(matrix(c(1,2,3,4),2,2)) # optional 4 graphs/page
plot(ladies.lm)

calc.relimp(ladies.lm,type=c("lmg","last","first","pratt"),
            rela=TRUE)
boot <- boot.relimp(ladies.lm, b = 1000, type = c("lmg",
                                            "last", "first", "pratt"), rank = TRUE,
                    diff = TRUE, rela = TRUE)
booteval.relimp(boot) # print result
plot(booteval.relimp(boot,sort=Fa)) # plot result

newstuff = data.frame(pavg_points = 80, pelopct=100)
predict(ladies.lm, newstuff)

ladies$prediction = predict(ladies.lm, ladies)^exp(1)


