library(readxl)
library(fmsb)
library(ggplot2)


ladies_x <- read_excel("/Users/syverjohansen/ski/elo/python/biathlon/radar/varmen_all_k.xlsx", 
                       sheet = "Sheet1", col_names = TRUE, na = "NA")


ladies <- data.frame(ladies_x)

#ladies <- ladies[which(ladies$pelopct>=85), ]
ladies <- ladies[which(ladies$season>=2018),]

ggplot(ladies, aes(x=pelopct, y=points^(1/exp(1)))) + geom_point()
ladies.lm <- lm((points) ~ ((pelopct)), data=ladies)#, family="gaussian")
summary(ladies.lm)
ladies.lm <- lm((points)^(1/3) ~ ((pelopct)), data=ladies, family="gaussian")
summary(ladies.lm)
ladies.lm <- lm((points)^(1/exp(1)) ~ ((pelopct)), data=ladies, family="gaussian")
summary(ladies.lm)
confint(ladies.lm)

ladies.lm <- lm(placepct ~ pelopct, data=ladies)
summary(ladies.lm)
ggplot(ladies, aes(x=pelopct, y=placepct)) + geom_point()


res <- resid(ladies.lm)
plot(fitted(ladies.lm), res)
qqnorm(res)
qqline(res)
plot(density(res))
hist(res)

