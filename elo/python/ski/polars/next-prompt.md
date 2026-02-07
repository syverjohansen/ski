1) Changing k-value in elo.py to for number of racers and not number of races
2) Creating all_scrape.py and all_update_scrape.py which has the different URL
3) Only cross-country uses Russia scrape or combined scrape
4) Creating all_elo.py, all_elo_script.sh, elo_predict.py, elo_predict_script.sh, elo_dynamic.py and elo_dynamic_script.sh
5) Made changes to startlist-scrape-races.py, startlist-scrape-weekend.py, and all the relay startlist-scrapes to read in pred chrono
6) Create chrono_predict.py and chrono_dynamic.py
6) Made changes to race-picks.R, weekly-picks2.R and any relay picks to read in the elo from the startlist scraper
7) Also need to make changes to startlist-scrape-champs.py, champs-predictions.R
8) Do the same for relays.  Easiest to copy over from the non-relay directory and look at how cross-country ski handles the relay files vs non-relay files.  


Ok let's go through this plan.  

1. This is right.  Let's make sure to follow the cross country model and comment out the one that's already there that uses races
2. Kind of?  It should be copying the scrape.py from biathlon and make the changes that differentiate scrape.py from all_scrape.py in cross-country since the formats to scrape are different.
3. Also kind of.  There are different elo types and intricacies between cross-country and biathlon.  See #2 for probably the better way of doing it.  Maybe for #2 and #3 you tell me what you think the differences are before they're implemented
4. Yes
5-9.  See 2-4
10-13 Yes
14. Correct although unlike cross-country biathlon includes relays in race-picks.R, so we will cross that bridge once we get there.
15. See 14
16. Yes
17. No relay R files.

Next things to do:
Can we add all_scrape_update.py?  Look at cross-country's as an example.