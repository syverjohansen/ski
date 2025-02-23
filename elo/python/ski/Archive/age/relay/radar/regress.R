library(readxl)
library(fmsb)
library(ggplot2)


ladies_x <- read_excel("/Users/syverjohansen/ski/elo/python/ski/relay/radar/varmen_sprint_k.xlsx", 
                       sheet = "Sheet1", col_names = TRUE, na = "NA")


ladies <- data.frame(ladies_x)

##Optional, but make pelopct the comparise against the alltime maximum
#max_elo = max(ladies$team_elo)
#ladies$pelopct = ladies$team_pelo/max_elo

#ladies <- ladies[which(ladies$pelopct>=85), ]
ladies <- ladies[which(ladies$season>=2018),]
#ladies <- ladies[which(ladies$ms==1),]
ladies <- ladies[which(ladies$pelo!=2600), ]
print(sum(ladies$points^(1/exp(1))))

ggplot(ladies, aes(x=team_pelo, y=points^(1/exp(1)))) + geom_point()
ggplot(ladies, aes(x=pelopct, y=points^(1/exp(1)))) + geom_point()
#ladies.lm <- lm((points) ~ ((pelopct)), data=ladies)#, family="gaussian")
ladies.lm <- lm((points)^(2/4) ~ pelopct, data=ladies, family="gaussian")
#summary(ladies.lm)
ladies.lm <- lm((points)^(1/exp(1)) ~ ((pelopct)), data=ladies, family="gaussian")
#ladies.lm
summary(ladies.lm)
confint(ladies.lm)


ladies$i

res <- resid(ladies.lm)
plot(fitted(ladies.lm), res)
qqnorm(res)
qqline(res)
plot(density(res))
hist(res)

