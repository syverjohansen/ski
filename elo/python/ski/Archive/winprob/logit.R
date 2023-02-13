library(readxl)
library(ggplot2)
library(aod)
win_prob = read_excel("~/ski/elo/python/ski/winprob/varladieswinprob.xlsx", 
                      col_names = TRUE, na = "NA")
print(win_prob)
win_probdf = data.frame(win_prob)
win_probdf = win_prob[,2:3]

win_probdf$win_prob = as.double(as.character(win_probdf$win_prob))*100
win_probdf$win = as.double(as.character(win_probdf$win))
ggplot(win_probdf, aes(win_prob, win)) + geom_point() + geom_smooth(method="glm", method.args=list(family="binomial"), se=FALSE)

win_logit <- glm(win ~ win_prob, data=win_probdf)
summary(win_logit)
confint(win_logit)
#confint.default(win_logit)

#wald.test(b=coef(win_logit), Sigma = vcov(win_logit), Terms=2)
exp(coef(win_logit))
